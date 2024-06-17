#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <vector>

#include "value_segment.h"
#include "fastpfor/fastpfor.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

extern thread_local FastPForLib::FastPFor<4> fastpfor128;

class IntValueSegment : public ValueSegment {
 public:
  static const uint8_t SUBFORMAT_ID_RAW = 0;
  static const uint8_t SUBFORMAT_ID_DELTAZG_FPF128_VB = 1;
  static const uint8_t SUBFORMAT_ID_DELTAZG_VB = 2;

  IntValueSegment(const Slice &slice, size_t &pos);
  void WriteTo(std::string &slice);
  void MergeFrom(const rocksdb::Slice &slice, size_t &pos);
  size_t MaxSerializedSize() { return 5 + 4 * values.size(); }

 private:
  bool is_signed;
  std::vector<int32_t> values;

  IntValueSegment();
  void writeCompressed(std::string &buf);
  void writeRaw(std::string &slice);
  void writeHeader(int subFormatId, std::string &slice);

  rocksdb::Status parseRaw(const rocksdb::Slice &slice, size_t &pos, size_t n);
  rocksdb::Status parseCompressed(const rocksdb::Slice &slice, size_t &pos,
                                  size_t n, int subFormatId);

  // static thread_local FastPForLib::SIMDFastPFor<4> fastpfor128;
};

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
