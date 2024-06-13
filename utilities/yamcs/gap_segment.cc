#include "gap_segment.h"

#include <rocksdb/slice.h>

#include "fastpfor/fastpfor.h"
#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

GapSegment::GapSegment(const Slice &slice, size_t &pos) {
  MergeFrom(slice, pos);
}

void GapSegment::WriteTo(std::string &buf) {
  //segStartIdxInsideInterval = 0 after merging
  write_var_u32(buf, 0);

  size_t pos = buf.size();
  buf.push_back(SUBFORMAT_ID_DELTAZG_FPF128_VB);

  std::vector<uint32_t> ddz = encodeDeltaDeltaZigZag(values);

  bool with_fastpfor = write_vec_u32_compressed(buf, ddz);
  if (!with_fastpfor) {
    buf[pos] = SUBFORMAT_ID_DELTAZG_VB;
  }
}

void GapSegment::MergeFrom(const rocksdb::Slice &slice, size_t &pos) {
  uint32_t intervalIdx;
  status = read_var_u32(slice, pos, intervalIdx);
  if (!status.ok()) {
    return;
  }
  
  uint8_t subFormatId = slice.data()[pos++];
  std::vector<uint32_t> ddz;
  status = read_vec_u32_compressed(
      subFormatId == SUBFORMAT_ID_DELTAZG_FPF128_VB, slice, pos, ddz);
  if (!status.ok()) {
    return;
  }
  if ((values.size() + (size_t)ddz.size()) > INT32_MAX) {
    status = Status::CompactionTooLarge("resulting segment would be too large");
    return;
  }
  auto n = values.size();
  decodeDeltaDeltaZigZag(ddz, values);

  if (intervalIdx > 0) {
    for (auto i = n; i < values.size(); i++) {
      values[i] += intervalIdx;
    }
  }
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
