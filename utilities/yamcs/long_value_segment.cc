#include "long_value_segment.h"

#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

LongValueSegment::LongValueSegment(Type type)
    : numericType(type) {}

LongValueSegment::LongValueSegment() : numericType(0) {}


void LongValueSegment::WriteTo(std::string& buf) {
  writeHeader(SUBFORMAT_ID_RAW, buf);
  int n = values.size();
  writeVarInt32(buf, n);

  for (int i = 0; i < n; i++) {
    write_u64_be(buf, values[i]);
  }
}

void LongValueSegment::writeHeader(int subFormatId, std::string& buf) {
  uint8_t x = (numericType << 4) | subFormatId;
  buf.push_back(x);
}

rocksdb::Status LongValueSegment::MergeFrom(const rocksdb::Slice& slice,
                                        size_t& pos) {
  uint8_t x = slice.data()[pos++];

  int subFormatId = x & 0xF;
  if (subFormatId != SUBFORMAT_ID_RAW) {
    return Status::Corruption("Unknown subformatId " +
                              std::to_string(subFormatId) +
                              " for LongValueSegment");
  }

  numericType = (x >> 4) & 3;
  uint32_t n;
  Status s = readVarInt32(slice, pos, n);
  if (!s.ok()) {
    return s;
  }

  if (pos + 8 * n > slice.size()) {
    return Status::Corruption(
        "Cannot decode long segment: expected " + std::to_string(8 * n) +
        " bytes and only " + std::to_string(slice.size() - pos) + " available");
  }

  values.resize(n);
  for (size_t i = 0; i < n; i++) {
    values[i] = read_u64_be_unchecked(slice, pos);
  }

  return Status::OK();
}

int LongValueSegment::getMaxSerializedSize() {
  return 4 + 8 * values.size();  // 4 for the size plus 8 for each element
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
