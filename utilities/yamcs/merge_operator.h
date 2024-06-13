//  Copyright (c) 2024 Nicolae Mihalache.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

/**
 * A MergeOperator for rocksdb that implements parameter archive segments merge.
 */
class YamcsParchiveMergeOperator : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value, Logger* logger) const override;

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "YamcsParchiveMergeOperator"; }

  bool AllowSingleOperand() const override { return true; }

  template <typename Iterator>
  bool MergeSlices(const Slice& key, const Slice& first_value, Iterator begin,
                   Iterator end, std::string* new_value, Logger* logger) const;

  //SortedTimeValueSegmentV1 stores times relative to the segment start; it is obsolete
  // static const uint8_t FID_SortedTimeValueSegmentV1 = 1;
  static const uint8_t FID_ParameterStatusSegment = 2;
  static const uint8_t FID_IntValueSegment = 11;
  static const uint8_t FID_StringValueSegment = 13;
  static const uint8_t FID_FloatValueSegment = 16;
  static const uint8_t FID_DoubleValueSegment = 17;
  static const uint8_t FID_LongValueSegment = 18;
  static const uint8_t FID_BinaryValueSegment = 19;
  static const uint8_t FID_BooleanValueSegment = 20;
  //SortedTimeValueSegmentV2 stores times relative to the interval start, so they can be merged just appending the values
  static const uint8_t FID_SortedTimeValueSegmentV2 = 21;
  static const uint8_t FID_GapSegment = 22;
};
}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
