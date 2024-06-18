#include "int_value_segment.h"

#include <rocksdb/slice.h>

#include "fastpfor/fastpfor.h"
#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

static bool get_signed(uint8_t x) { return (((x >> 4) & 1) == 1); }


static uint8_t header(bool is_signed, int subFormatId) {
  uint8_t x = is_signed ? 1 : 0;
  return (x << 4) | subFormatId;
}
IntValueSegment::IntValueSegment(const Slice &slice, size_t &pos)
    : is_signed(get_signed(slice.data()[pos])) {
  MergeFrom(slice, pos);
}

void IntValueSegment::WriteTo(std::string &buf) {
  size_t buf_pos = buf.size();
  writeCompressed(buf);
  if (buf.size() > buf_pos + values.size() * 4) {
    buf.resize(buf_pos);
    writeRaw(buf);
  }
}

void IntValueSegment::writeCompressed(std::string &buf) {
  size_t pos = buf.size();
  buf.push_back(header(is_signed, SUBFORMAT_ID_DELTAZG_FPF128_VB));

  std::vector<uint32_t> ddz = encodeDeltaDeltaZigZag(values);
  
  bool with_fastpfor = write_vec_u32_compressed(buf, ddz);
  if (!with_fastpfor) {
    buf[pos] = (header(is_signed, SUBFORMAT_ID_DELTAZG_VB));
  }
}

void IntValueSegment::writeRaw(std::string &buf) {
  buf.push_back(header(is_signed, SUBFORMAT_ID_RAW));
  write_var_u32(buf, values.size());
  int n = values.size();
  for (int i = 0; i < n; i++) {
    write_u32_be(buf, values[i]);
  }
}


void IntValueSegment::MergeFrom(const rocksdb::Slice &slice, size_t &pos) {
  uint8_t x = slice.data()[pos++];
  int subFormatId = x & 0xF;
  if (is_signed != get_signed(x)) {
    if (is_signed) {
      status = Status::Corruption(
          "Expected signed data but the merge contains unsigned");
    } else {
      status = Status::Corruption(
          "Expected unsigned data but the merge contains signed");
    }
    return;
  }

  switch (subFormatId) {
    case SUBFORMAT_ID_RAW:
      parseRaw(slice, pos);
      break;
    case SUBFORMAT_ID_DELTAZG_FPF128_VB:
    case SUBFORMAT_ID_DELTAZG_VB:
      parseCompressed(slice, pos, subFormatId);
      break;
    default:
      status = Status::Corruption("Unknown subformatId: " +
                                  std::to_string(subFormatId));
  }
}

void IntValueSegment::parseRaw(const rocksdb::Slice &slice, size_t &pos) {
  uint32_t n;
  status = read_var_u32(slice, pos, n);
  if (!status.ok()) {
    return;
  }

  if (n == 0) {
    status = Status::Corruption("Encountered segment with 0 elements");
    return;
  }
  if (pos + 4 * n > slice.size()) {
    status = Status::Corruption(
        "Cannot decode long segment: expected " + std::to_string(4 * n) +
        " bytes and only " + std::to_string(slice.size() - pos) + " available");
    return;
  }

  values.reserve(values.size() + n);
  for (size_t i = 0; i < n; i++) {
    values.push_back(read_u32_be_unchecked(slice, pos));
  }
}

// n is the number of values to be read
void IntValueSegment::parseCompressed(const rocksdb::Slice &slice, size_t &pos,
                                      int subFormatId) {
  std::vector<uint32_t> ddz;
  status = read_vec_u32_compressed(
      subFormatId == SUBFORMAT_ID_DELTAZG_FPF128_VB, slice, pos, ddz);
  if (!status.ok()) {
    return;
  }
  decodeDeltaDeltaZigZag(ddz, values);
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
