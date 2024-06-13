#include "long_value_segment.h"

#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

LongValueSegment::Type get_type(uint8_t x) {
  return (LongValueSegment::Type)(x >> 4);
}
LongValueSegment::LongValueSegment(const Slice& slice, size_t& pos)
    : type(get_type(slice.data()[pos])) {
  MergeFrom(slice, pos);
}

void LongValueSegment::WriteTo(std::string& buf) {
  uint32_t n = (uint32_t)values.size();

  writeHeader(SUBFORMAT_ID_RAW, buf);
  write_var_u32(buf, n);

  for (auto v : values) {
    write_u64_be(buf, v);
  }
}

void LongValueSegment::writeHeader(uint8_t subFormatId, std::string& buf) {
  uint8_t t = static_cast<uint8_t>(type);
  uint8_t x = (t << 4) | subFormatId;
  buf.push_back(x);
}

void LongValueSegment::MergeFrom(const rocksdb::Slice& slice, size_t& pos) {
  uint8_t x = slice.data()[pos++];

  int subFormatId = x & 0xF;
  if (subFormatId != SUBFORMAT_ID_RAW) {
    status = Status::Corruption("Unknown subformatId " +
                                std::to_string(subFormatId) +
                                " for LongValueSegment");
    return;
  }

  Type _type = get_type(x);
  if (type != _type) {
    status = Status::Corruption(
        "Required to merge a segment of type " + std::to_string(_type) +
        " to a segment of type " + std::to_string(type));
    return;
  }
  uint32_t n;
  status = read_var_u32(slice, pos, n);
  if (!status.ok()) {
    return;
  }

  if ((values.size() + (size_t)n) > INT32_MAX) {
    status = Status::CompactionTooLarge("resulting segment would be too large");
    return;
  }

  if (pos + 8 * n > slice.size()) {
    status = Status::Corruption(
        "Cannot decode long segment: expected " + std::to_string(8 * n) +
        " bytes and only " + std::to_string(slice.size() - pos) + " available");
    return;
  }

  values.reserve(values.size() + n);
  for (size_t i = 0; i < n; i++) {
    values.push_back(read_u64_be_unchecked(slice, pos));
  }
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
