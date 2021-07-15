#pragma once

#include <cstdint>
#include <optional>
#include <tuple>
#include <variant>
#include <string>

#include "char_vector.h"
#include "mmaped.h"

namespace RoflDb {

struct Key : public Utils::ZeroCopyCharVector {
public:
    Key(const std::byte* memAddress, std::size_t length) : ZeroCopyCharVector(memAddress, length) {};
    [[nodiscard]] inline std::strong_ordering operator<=>(const Key& other) const;
    [[nodiscard]] inline bool operator==(const Key& other) const;
};

struct Blob : public Utils::ZeroCopyCharVector {
public:
    Blob(const std::byte* memAddress, std::size_t length) : ZeroCopyCharVector(memAddress, length) {};
};

namespace priv {
    struct DataLink {
    public:
        uint64_t offset;
        explicit DataLink(uint64_t offset) : offset(offset) {}
    };

    struct NodeLink {
        uint32_t offset;
        explicit NodeLink(uint32_t offset) : offset(offset) {}
    };

    class Record : public Utils::Mmaped<uint16_t> {
    private:
        typedef uint16_t KeyLengthType;

        enum Type : uint8_t { DATA = 0, NODE = 1 };

    public:
        [[nodiscard]] inline Key getKey() const;
        [[nodiscard]] inline std::variant<DataLink, NodeLink> getContentLink() const;
    };

    class Node : public Utils::Mmaped<uint32_t> {
    public:
        [[nodiscard]] inline const Record* approxGet(const Key& key) const;
    };

    class Tree : public Utils::Mmaped<uint64_t> {
    public:
        [[nodiscard]] std::tuple<const Node*, const Record*> approxGet(const Key& key) const;
    };

    class DataHeap : public Utils::Mmaped<uint64_t> {
    public:
        [[nodiscard]] inline Blob getData(std::size_t offset) const;
    };
}

class DbReader {
protected:
    static constexpr std::byte MAGIC[4] = {
        static_cast<const std::byte>('R'),
        static_cast<const std::byte>('O'),
        static_cast<const std::byte>('F'),
        static_cast<const std::byte>('L'),
    };

    const priv::Tree* tree;
    const priv::DataHeap* dataHeap;

public:
    DbReader(std::byte* memAddress, std::size_t memLength);
    [[nodiscard]] std::optional<Blob> get(const Key& key) const;
    [[nodiscard]] std::optional<Blob> get(const std::string& key) const;
};

}
