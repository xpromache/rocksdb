#include "long_value_segment.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

LongValueSegment::LongValueSegment(Type type)
    : numericType(getNumericType(type)) {}

LongValueSegment::LongValueSegment()
    : numericType(0) {}


int LongValueSegment::getNumericType(Type type) {
    for (int i = 0; i < 3; i++) {
        if (types[i] == type) {
            return i;
        }
    }
    throw std::invalid_argument("Invalid Type");
}

void LongValueSegment::writeTo(rocksdb::Slice& slice) {
    writeHeader(SUBFORMAT_ID_RAW, slice);
    int n = values.size();
    VarIntUtil::writeVarInt32(slice, n);
    for (int i = 0; i < n; i++) {
        // Assuming slice is writable
        *reinterpret_cast<int64_t*>(const_cast<char*>(slice.data() + slice.size())) = values[i];
    }
}

void LongValueSegment::writeHeader(int subFormatId, rocksdb::Slice& slice) {
    int x = (numericType << 4) | subFormatId;
    *reinterpret_cast<uint8_t*>(const_cast<char*>(slice.data() + slice.size())) = static_cast<uint8_t>(x);
}

void LongValueSegment::parse(rocksdb::Slice& slice) {
    uint8_t x = *reinterpret_cast<const uint8_t*>(slice.data());
    int subFormatId = x & 0xF;
    if (subFormatId != SUBFORMAT_ID_RAW) {
        throw DecodingException("Unknown subformatId " + std::to_string(subFormatId) + " for LongValueSegment");
    }

    numericType = (x >> 4) & 3;
    int n = VarIntUtil::readVarInt32(slice);

    if (slice.size() - slice.data() < 8 * n) {
        throw DecodingException("Cannot decode long segment: expected " + std::to_string(8 * n) +
            " bytes and only " + std::to_string(slice.size() - slice.data()) + " available");
    }

    values.resize(n);
    for (int i = 0; i < n; i++) {
        values[i] = *reinterpret_cast<const int64_t*>(slice.data() + i * sizeof(int64_t));
    }
}

LongValueSegment LongValueSegment::parseFrom(rocksdb::Slice& slice) {
    LongValueSegment r;
    r.parse(slice);
    return r;
}

int LongValueSegment::getMaxSerializedSize() {
    return 4 + 8 * values.size(); // 4 for the size plus 8 for each element
}

} // namespace yamcs
} // namespace ROCKSDB_NAMESPACE
