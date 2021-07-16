#pragma once

#include <cstddef>

namespace RoflDb::Utils {

struct ZeroCopyCharVector {
protected:
    const std::byte* const memAddress;
    const std::size_t length;

public:
    ZeroCopyCharVector(const std::byte* memAddress, std::size_t length) : memAddress(memAddress), length(length) {};

    [[nodiscard]] const inline std::byte* get() {
        return memAddress;
    }

    [[nodiscard]] inline std::size_t size() const {
        return length;
    }

    [[nodiscard]] inline explicit operator bool() const {
        return length > 0;
    }
};

}
