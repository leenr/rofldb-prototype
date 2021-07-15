#pragma once

#include <type_traits>
#include "exceptions.h"

namespace RoflDb::Utils {

    template<class SizeT>
    class Mmaped;

    std::false_type is_mmaped_impl(...);
    template<typename SizeType>
    std::true_type is_mmaped_impl(Mmaped<SizeType>*);
    template<typename SizeType>
    std::true_type is_mmaped_impl(const Mmaped<SizeType>*);

    template<typename T>
    using is_mmaped = decltype(is_mmaped_impl(std::declval<T*>()));

    class PayloadReader {
        const std::byte* address;
        uint32_t remaining;

    public:
        PayloadReader(const PayloadReader& other) = default;
        PayloadReader(const std::byte* address, const uint32_t boundSize) : address(address), remaining(boundSize) {}

        template<class ReadT>
        [[nodiscard]] inline std::enable_if_t<!is_mmaped<ReadT>::value, ReadT> read(std::size_t offset = 0) {
            if (offset > 0) {
                skip(offset);
            }

            auto res = *(ReadT*)skip<ReadT>();

#pragma clang diagnostic push
#pragma ide diagnostic ignored "Simplify"
            // convert from big endian to host endianness
            if constexpr (__BYTE_ORDER == __BIG_ENDIAN && std::is_integral_v<ReadT> && sizeof(ReadT) > 1) {
                auto* resPtr = reinterpret_cast<std::byte*>(&res);
                std::reverse(resPtr, resPtr + sizeof(ReadT));
            }
#pragma clang diagnostic pop

            return res;
        }

        template<class ReadT>
        [[nodiscard]] inline std::enable_if_t<is_mmaped<ReadT>::value, ReadT*> read(std::size_t offset = 0) {
            if (offset > 0) {
                skip(offset);
            }
            return (ReadT*)skip<ReadT>();
        }

        template<class SkipT>
        inline std::byte* skip() {
            if constexpr (is_mmaped<SkipT>()) {
                return skip(sizeof(SkipT) + ((SkipT*)address)->getSize());
            } else {
                return skip(sizeof(SkipT));
            }
        }

        inline std::byte* skip(std::size_t bytes) {
#if ROFLDB_SECURITY
            if (bytes > remaining) [[ unlikely ]] {
                throw Exceptions::data_corrupted_error("Read out of bounds");
            }
#endif
            address += bytes;
            remaining -= bytes;
            return const_cast<std::byte*>(address - bytes);
        }

        [[nodiscard]] inline const std::byte* getAddress() const {
            return address;
        }

        [[nodiscard]] inline explicit operator bool() const {
            return remaining > 0;
        }
    };

    template<class SizeT>
    class Mmaped {
        static_assert(!std::is_pointer_v<SizeT> && std::is_unsigned_v<SizeT>);

    protected:
        static constexpr std::size_t PAYLOAD_OFFSET = sizeof(SizeT);
        [[nodiscard]] inline PayloadReader getPayloadReader() const {
            return PayloadReader((std::byte*)this + PAYLOAD_OFFSET, getSize());
        }
        [[nodiscard]] inline SizeT getSize() const {
            return PayloadReader((std::byte*)this, PAYLOAD_OFFSET).read<SizeT>();
        }

        friend class PayloadReader;
    };
}
