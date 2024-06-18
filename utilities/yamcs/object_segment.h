#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "value_segment.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

class ObjectSegment : public ValueSegment {
 public:
  static const uint8_t SUBFORMAT_ID_RAW = 0;
  static const uint8_t SUBFORMAT_ID_ENUM_RLE = 1;
  static const uint8_t SUBFORMAT_ID_ENUM_VB = 2;
  static const uint8_t SUBFORMAT_ID_ENUM_FPROF = 3;

  ObjectSegment(const Slice& slice, size_t& pos);
  void WriteTo(std::string& slice);
  void MergeFrom(const rocksdb::Slice& slice, size_t& pos);
  size_t MaxSerializedSize() { return 4 + 8 * values.size(); }

 private:
  void writeHeader(int subFormatId, std::string& slice);
  void mergeRaw(const rocksdb::Slice& slice, size_t& pos);
  void parseEnum(const rocksdb::Slice& slice, size_t& pos);
  void parseEnumRle(const rocksdb::Slice& slice, size_t& pos);
  void parseEnumNonRle(const rocksdb::Slice& slice, size_t& pos);
  void mergeRleEnum(const rocksdb::Slice& slice, size_t& pos);
  void mergeNonRleEnum(int newSliceFid, const rocksdb::Slice& slice,
                       size_t& pos);
  std::unordered_map<size_t, size_t> AddEnumValues(
      const std::vector<std::string_view>& tmp_values);

  // initialized at construction time, all the merged segment are adapted to
  // this
  const int formatId;
  // used in all cases but for enums it stores only the unique values
  std::vector<std::string_view> values;

  // used for enums
  // maps the values to their index in the values list
  std::unordered_map<std::string_view, size_t> valueIndexMap;

  // used for RLE enum
  std::vector<uint32_t> rle_counts;
  std::vector<uint32_t> rle_values;

  // used for the other enums
  std::vector<uint32_t> values_idx;
};

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
