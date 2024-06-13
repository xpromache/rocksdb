#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <vector>

#include "value_segment.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

class GapSegment : public ValueSegment {
 public:
  static const uint8_t SUBFORMAT_ID_DELTAZG_FPF128_VB = 1;
  static const uint8_t SUBFORMAT_ID_DELTAZG_VB = 2;

  GapSegment(const Slice &slice, size_t &pos);
  void WriteTo(std::string &slice);
  void MergeFrom(const rocksdb::Slice &slice, size_t &pos);
  size_t MaxSerializedSize() { return 9 + 4 * values.size(); }

 private:
  std::vector<int32_t> values;
  void writeCompressed(std::string &buf);
 
  void parseCompressed(const rocksdb::Slice &slice, size_t &pos,
                                  int subFormatId);
};

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
