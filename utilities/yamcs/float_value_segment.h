#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <vector>

#include "value_segment.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

class FloatValueSegment : public ValueSegment {
 public:
  static const uint8_t SUBFORMAT_ID_RAW = 0;
  static const uint8_t SUBFORMAT_ID_COMPRESSED = 1;

  FloatValueSegment(const Slice &slice, size_t &pos);
  
  void WriteTo(std::string& slice);
  void MergeFrom(const rocksdb::Slice& slice, size_t& pos);
  size_t MaxSerializedSize() { return 5 + 4 * values.size() + 1; }

 private:
  void parseRaw(const rocksdb::Slice& slice, int n);
  void writeRaw(std::string& slice);
  void writeCompressed(std::string& buf);
  std::vector<float> values;
};

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
