#include "float_value_segment.h"

#include "float_compress.h"
#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

FloatValueSegment::FloatValueSegment(const Slice& slice, size_t& pos) {
  MergeFrom(slice, pos);
}

void FloatValueSegment::WriteTo(std::string& buf) {
  size_t buf_pos = buf.size();

  writeCompressed(buf);
  if (buf.size() > 4 + buf_pos + values.size() * 4) {
    buf.resize(buf_pos);
    writeRaw(buf);
  }
}

void FloatValueSegment::writeRaw(std::string& buf) {
  buf.push_back(SUBFORMAT_ID_RAW);
  write_var_u32(buf, (uint32_t)values.size());
  for (float v : values) {
    write_f32_be(buf, v);
  }
}

void FloatValueSegment::writeCompressed(std::string& buf) {
  buf.push_back(SUBFORMAT_ID_COMPRESSED);
  write_var_u32(buf, (uint32_t)values.size());
  float_compress(values, buf);
}

void FloatValueSegment::MergeFrom(const rocksdb::Slice& slice, size_t& pos) {
  uint8_t x = slice.data()[pos++];

  int subFormatId = x & 0xF;
  uint32_t n;
  status = read_var_u32(slice, pos, n);
  if (!status.ok()) {
    return;
  }

  if ((values.size() + (size_t)n) > INT32_MAX) {
    status = Status::CompactionTooLarge("resulting segment would be too large");
    return;
  }
  values.reserve(values.size() + n);

  if (subFormatId == SUBFORMAT_ID_RAW) {
    if (pos + 4 * n > slice.size()) {
      status =
          Status::Corruption("Cannot decode float segment: expected " +
                             std::to_string(4 * n) + " bytes and only " +
                             std::to_string(slice.size() - pos) + " available");
      return;
    }
   
    for (size_t i = 0; i < n; i++) {
      values.push_back(read_f32_be_unchecked(slice, pos));
    }
  } else if (subFormatId == SUBFORMAT_ID_COMPRESSED) {
    status = float_decompress(slice, pos, n, values);
  } else {
    status = Status::Corruption("Unknown subformatId " +
                                std::to_string(subFormatId) +
                                " for FloatValueSegment");
  }
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
