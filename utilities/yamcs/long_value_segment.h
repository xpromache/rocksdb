#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <vector>

#include "value_segment.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

class LongValueSegment : public ValueSegment {
 public:
  enum Type {
    UINT64,
    SINT64,
    TIMESTAMP,
  };
  static const uint8_t SUBFORMAT_ID_RAW = 0;

  LongValueSegment(const Slice& slice, size_t& pos);
  void WriteTo(std::string& slice);
  void MergeFrom(const rocksdb::Slice& slice, size_t& pos);
  size_t MaxSerializedSize() { return 4 + 8 * values.size(); }

 private:
  void writeHeader(uint8_t subFormatId, std::string& slice);
  void parseRaw(const rocksdb::Slice& slice, int n);

  std::vector<int64_t> values;
  Type type;  // index in the array
};

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
