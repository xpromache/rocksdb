#include "double_value_segment.h"

#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

DoubleValueSegment::DoubleValueSegment(const Slice& slice, size_t& pos) {
  MergeFrom(slice, pos);
}
void DoubleValueSegment::WriteTo(std::string& buf) {
  int n = values.size();

  buf.push_back(SUBFORMAT_ID_RAW);
  write_var_u32(buf, n);

  for (int i = 0; i < n; i++) {
    write_f64_be(buf, values[i]);
  }
}

void DoubleValueSegment::MergeFrom(const rocksdb::Slice& slice, size_t& pos) {
  uint8_t x = slice.data()[pos++];

  int subFormatId = x & 0xF;
  if (subFormatId != SUBFORMAT_ID_RAW) {
    status = Status::Corruption("Unknown subformatId " +
                                std::to_string(subFormatId) +
                                " for DoubleValueSegment");
    return;
  }

  uint32_t n;
  status = read_var_u32(slice, pos, n);
  if (!status.ok()) {
    return;
  }

  if (pos + 8 * n > slice.size()) {
    status = Status::Corruption(
        "Cannot decode double segment: expected " + std::to_string(8 * n) +
        " bytes and only " + std::to_string(slice.size() - pos) + " available");
    return;
  }

  values.reserve(values.size() + n);
  for (size_t i = 0; i < n; i++) {
    values.push_back(read_f64_be_unchecked(slice, pos));
  }
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
