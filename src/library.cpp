#include <cstring>
#include <limits>
#include <cassert>

#include "../include/exceptions.h"
#include "../include/library.h"

namespace RoflDb {

// Key =================================================================================================================

    std::strong_ordering Key::operator<=>(const Key& other) const {
        int cmpResult = std::memcmp(this->memAddress, other.memAddress, std::min(this->length, other.length));
        if (cmpResult > 0) [[unlikely]] {
            return std::strong_ordering::greater;
        } else if (cmpResult < 0) [[likely]] {
            return std::strong_ordering::less;
        } else [[ unlikely ]] {
            return this->length <=> other.length;
        }
    }

    bool Key::operator==(const Key& other) const {
        return this->operator<=>(other) == std::strong_ordering::equal;
    }

// END Key =============================================================================================================


// priv::ValueCollection ===============================================================================================

    Value priv::ValueCollection::getByOffset(ValueOffsetType offset) const {
        return getPayloadReader().read<Value>(offset);
    }

// END priv::ValueCollection ===========================================================================================


// priv::Tree::Node ====================================================================================================

    std::optional<priv::Tree::Node::Match> priv::Tree::Node::match(const Key& searchKey) const {
        auto payloadReader = getPayloadReader();

        auto nodeKey = payloadReader.read<Key>();
        auto keyCompareResult = searchKey.operator<=>(nodeKey);

        auto valueOffset = payloadReader.read<ValueCollection::ValueOffsetType>();
        if (keyCompareResult == std::strong_ordering::equal) [[unlikely]] {
            return ValueMatch(valueOffset);
        }

        // left
        if (!payloadReader) [[unlikely]] {
            return std::nullopt;
        }
        auto leftOffset = payloadReader.read<Node::OffsetType>();
        assert(leftOffset > 0);
        if (keyCompareResult == std::strong_ordering::less) {
            return DropDownMatch(leftOffset);
        }

        // right
        if (!payloadReader) {
            return std::nullopt;
        }
        auto rightOffset = payloadReader.read<Node::OffsetType>();
        assert(rightOffset > 0);
        return DropDownMatch(rightOffset);
    }

// END priv::Tree::Node ================================================================================================


// priv::Tree ==========================================================================================================

    std::optional<priv::ValueCollection::ValueOffsetType> priv::Tree::get(const Key& key) const {
        std::optional<Node::OffsetType> offset = getPayloadReader().read<Node::OffsetType>();
        while (auto optionalMatch = getPayloadReader().read<const Node*>(*offset)->match(key)) [[likely]] {
            auto match = optionalMatch.value();
            if (holds_alternative<Node::ValueMatch>(match)) {
                return std::get<Node::ValueMatch>(match).valueOffset;
            }
            offset = std::get<Node::DropDownMatch>(match).nodeOffset;
        }
        return std::nullopt;
    }

// END priv::Tree ======================================================================================================


DbReader::DbReader(std::byte* memAddress, std::size_t memLength) {
    Utils::PayloadReader payloadReader(memAddress, memLength);
    if (std::memcmp(payloadReader.skip(sizeof MAGIC), MAGIC, sizeof MAGIC) != 0) {
        throw Exceptions::magic_error("Invalid file magic");
    }

    auto version = payloadReader.read<uint16_t>();
    if (version != 0) [[unlikely]] {
        throw Exceptions::magic_error("Invalid format version");
    }

    valueCollection = payloadReader.read<const priv::ValueCollection*>();
    tree = payloadReader.read<const priv::Tree*>();
}

std::optional<Value> DbReader::get(const Key& key) const {
    auto offset = tree->get(key);
    if (!offset.has_value()) [[unlikely]] {
        return std::nullopt;
    }
    return valueCollection->getByOffset(*offset);
}

std::optional<Value> DbReader::get(const std::string& key) const {
    return get(Key((std::byte*)key.c_str(), key.size()));
}

}
