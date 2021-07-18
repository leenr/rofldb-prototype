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
    using SizeType = uint16_t;

    Key(const std::byte* memAddress, std::size_t length) : ZeroCopyCharVector(memAddress, length) {};
    [[nodiscard]] inline std::strong_ordering operator<=>(const Key& other) const;
    [[nodiscard]] inline bool operator==(const Key& other) const;
};


struct Value : public Utils::ZeroCopyCharVector {
public:
    using SizeType = uint32_t;

    Value(const std::byte* memAddress, std::size_t length) : ZeroCopyCharVector(memAddress, length) {};
};


namespace priv {
    class ValueCollection : public Utils::Mmaped<ValueCollection, uint64_t> {
    public:
        using ValueOffsetType = SizeType;

        [[nodiscard]] inline Value getByOffset(ValueOffsetType offset) const;
    };

    class Tree : public Utils::Mmaped<Tree, uint32_t> {
    public:

        class Node : public Utils::Mmaped<Node, uint16_t> {
        public:
            using OffsetType = Tree::SizeType;

            struct ValueMatch {
                const ValueCollection::ValueOffsetType valueOffset;
            protected:
                inline explicit ValueMatch(ValueCollection::ValueOffsetType valueOffset) : valueOffset(valueOffset) {}
                friend class Node;
            };

            struct DropDownMatch {
                const Node::OffsetType nodeOffset;
            protected:
                inline explicit DropDownMatch(ValueCollection::ValueOffsetType nodeOffset) : nodeOffset(nodeOffset) {}
                friend class Node;
            };

            using Match = std::variant<ValueMatch, DropDownMatch>;
            [[nodiscard]] inline std::optional<Match> match(const Key& key) const;
        };

        [[nodiscard]] std::optional<ValueCollection::ValueOffsetType> get(const Key& key) const;
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

    const priv::ValueCollection* valueCollection;
    const priv::Tree* tree;

public:
    DbReader(std::byte* memAddress, std::size_t memLength);
    [[nodiscard]] std::optional<Value> get(const Key& key) const;
    [[nodiscard]] std::optional<Value> get(const std::string& key) const;
    [[nodiscard]] std::optional<Value> get(const std::vector<std::byte>& key) const;
};


}

namespace RoflDb::Utils {
    template<>
    inline std::size_t getReadSize<Key>(const std::byte* address) {
        auto size = Utils::read<Key::SizeType>(address);
        return sizeof(size) + size;
    }

    template<>
    inline std::size_t getReadSize<Value>(const std::byte* address) {
        auto size = Utils::read<Value::SizeType>(address);
        return sizeof(size) + size;
    }

    template<>
    inline Key read<Key>(const std::byte* address) {
        return Key(address + sizeof(Key::SizeType), Utils::read<Key::SizeType>(address));
    }

    template<>
    inline Value read<Value>(const std::byte* address) {
        return Value(address + sizeof(Value::SizeType), Utils::read<Value::SizeType>(address));
    }
}
