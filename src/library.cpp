#include <cstring>
#include <limits>

#include "../include/exceptions.h"
#include "../include/library.h"

namespace RoflDb {

std::strong_ordering Key::operator<=>(const Key& other) const {
    int cmpResult = std::memcmp(this->memAddress, other.memAddress, std::min(other.length, this->length));
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

Key priv::Record::getKey() const {
    auto payloadReader = getPayloadReader();
    auto keyLength = payloadReader.read<KeyLengthType>();
    return Key(payloadReader.getAddress(), keyLength);
}

std::variant<priv::DataLink, priv::NodeLink> priv::Record::getContentLink() const {
    auto payloadReader = getPayloadReader();

    auto keyLength = payloadReader.read<KeyLengthType>();
    payloadReader.skip(keyLength);

    auto type = payloadReader.read<Type>();
    switch (type) {
        case Type::NODE:
            return NodeLink(payloadReader.read<typeof NodeLink::offset>());
        case Type::DATA:
            return DataLink(payloadReader.read<typeof DataLink::offset>());
        default: [[unlikely]]
            throw Exceptions::data_corrupted_error("Data corrupted or an incompatible DB format: got an unknown Node Record type");
    }
}

const priv::Record* priv::Node::approxGet(const Key& key) const {
    auto payloadReader = getPayloadReader();
    while (payloadReader) [[likely]] {
        auto* record = payloadReader.read<const Record>();
        auto recordKey = record->getKey();
        if (!recordKey || recordKey >= key) [[unlikely]] {
            return record;
        }
    }
    return nullptr;
}

std::tuple<const priv::Node*, const priv::Record*> priv::Tree::approxGet(const Key& key) const {
    if (getSize() == 0) {
        // empty tree
        return {nullptr, nullptr};
    }
    auto* node = getPayloadReader().read<const Node>();
    while (const Record* record = node->approxGet(key)) [[likely]] {
        const auto contentLink = record->getContentLink();
        if (std::holds_alternative<DataLink>(contentLink)) [[unlikely]] {
            return {node, record};
        }
        node = getPayloadReader().read<const Node>(std::get<NodeLink>(contentLink).offset);
    }
    return {nullptr, nullptr};
}

Blob priv::DataDump::getData(std::size_t offset) const {
    auto payloadReader = getPayloadReader();
    payloadReader.skip(offset);
    auto length = payloadReader.read<uint32_t>();
    return Blob(payloadReader.skip(length), length);
}

DbReader::DbReader(std::byte* memAddress, std::size_t memLength) {
    Utils::PayloadReader payloadReader(memAddress, memLength);
    if (std::memcmp(payloadReader.skip(sizeof MAGIC), MAGIC, sizeof MAGIC) != 0) {
        throw Exceptions::magic_error("Invalid file magic");
    }

    auto version = payloadReader.read<uint16_t>();
    if (version != 0) [[unlikely]] {
        throw Exceptions::magic_error("Invalid format version");
    }

    tree = payloadReader.read<const priv::Tree>();
    dataDump = payloadReader.read<const priv::DataDump>();
}

std::optional<Blob> DbReader::get(const Key& key) const {
    auto [_, record] = tree->approxGet(key);
    if (record == nullptr || static_cast<const Key>(record->getKey()) != key) [[unlikely]] {
        return std::nullopt;
    }

    const auto contentLink = record->getContentLink();
    if (!std::holds_alternative<priv::DataLink>(contentLink)) [[unlikely]] {
        return std::nullopt;
    }

    return dataDump->getData(std::get<priv::DataLink>(contentLink).offset);
}

std::optional<Blob> DbReader::get(const std::string& key) const {
    return get(Key((std::byte*)key.c_str(), key.size()));
}

}
