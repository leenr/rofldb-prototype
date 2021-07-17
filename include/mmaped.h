#pragma once

#include <bit>
#include <type_traits>
#include "exceptions.h"

namespace RoflDb::Utils {

    template<class ThisT, class SizeT>
    class Mmaped;

    std::false_type is_mmaped_impl(...);
    template<class ThisT, class SizeType>
    [[maybe_unused]] std::true_type is_mmaped_impl(Mmaped<ThisT, SizeType>*);
    template<class ThisT, class SizeType>
    [[maybe_unused]] std::true_type is_mmaped_impl(const Mmaped<ThisT, SizeType>*);

    template<class T>
    using is_mmaped = decltype(is_mmaped_impl(std::declval<T*>()));
    template<class T>
    using is_mmaped_ptr = std::integral_constant<bool, std::is_pointer_v<T> && decltype(is_mmaped_impl(std::declval<T>()))::value>;

    template<class ReadT, std::enable_if_t<is_mmaped_ptr<ReadT>::value, bool> = true>
    inline std::size_t getReadSize(const std::byte* address) {
        auto size = reinterpret_cast<ReadT>(address)->getSize();
        return sizeof(size) + size;
    }

    template<class ReadT, std::enable_if_t<!std::is_pointer_v<ReadT>, bool> = true>
    inline std::size_t getReadSize(const std::byte* address) {
        return sizeof(ReadT);
    }

    template<class ReadT, std::enable_if_t<is_mmaped_ptr<ReadT>::value, bool> = true>
    [[nodiscard]] inline ReadT read(const std::byte* address) {
        return reinterpret_cast<ReadT>(address);
    }

    template<class ReadT, std::enable_if_t<!std::is_pointer_v<ReadT>, bool> = true>
    [[nodiscard]] inline ReadT read(const std::byte* address) {
        auto* resPtr = reinterpret_cast<const ReadT*>(address);

#pragma clang diagnostic push
#pragma ide diagnostic ignored "Simplify"
        // convert from big endian to host endianness
        if constexpr (std::is_integral_v<std::remove_pointer_t<ReadT>> && sizeof(ReadT) > 1 && std::endian::native == std::endian::big) {
            auto* bytesPtr = reinterpret_cast<std::byte*>(resPtr);
            std::reverse(bytesPtr, bytesPtr + sizeof(ReadT));
        }
#pragma clang diagnostic pop

        return *resPtr;
    }

    class PayloadReader {
        const std::byte* address;
        std::size_t remaining;

    public:
        PayloadReader(const PayloadReader& other) = default;
        PayloadReader(const std::byte* address, std::size_t boundSize) : address(address), remaining(boundSize) {}

        template<class ReadT>
        [[nodiscard]] inline ReadT read(std::size_t offset = 0) {
            skip(offset);
            auto size = Utils::getReadSize<ReadT>(getAddress());
            return Utils::read<ReadT>(skip(size));
        }

        inline const std::byte* skip(std::size_t bytes) {
#if ROFLDB_SECURITY
            if (bytes > remaining) [[unlikely]] {
                throw Exceptions::data_corrupted_error("Out of bounds");
            }
#endif
            address += bytes;
            remaining -= bytes;
            return address - bytes;
        }

        template<class SkipT>
        inline const std::byte* skip() {
            if constexpr (is_mmaped_ptr<SkipT>()) {
                auto size = ((SkipT*)address)->getSize();
                return skip(sizeof(size) + size);
            } else {
                return skip(sizeof(SkipT));
            }
        }

        [[nodiscard]] inline const std::byte* getAddress() const {
            return address;
        }

        [[nodiscard]] inline std::size_t getRemaining() const {
            return remaining;
        }

        [[nodiscard]] inline explicit operator bool() const {
            return getRemaining() > 0;
        }
    };

    template<class ThisT, class SizeT = void>
    class Mmaped {
        static_assert(!std::is_pointer_v<SizeT> && std::is_unsigned_v<SizeT>);

    public:
        using SizeType = SizeT;

        [[nodiscard]] inline SizeT getSize() const {
            return Utils::read<SizeT>(std::bit_cast<std::byte*>(this));
        }

    protected:
        static constexpr std::size_t PAYLOAD_OFFSET = sizeof(SizeT);

        [[nodiscard]] inline std::byte* getPayloadAddress() const {
            return (std::byte*)this + PAYLOAD_OFFSET;
        }
        [[nodiscard]] inline PayloadReader getPayloadReader() const {
            return PayloadReader(getPayloadAddress(), getSize());
        }
    };
}
