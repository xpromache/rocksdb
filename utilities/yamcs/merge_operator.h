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

  static const uint8_t SortedTimeValueSegment = 1;
  static const uint8_t ParameterStatusSegment = 2;
  static const uint8_t IntValueSegment = 11;
  static const uint8_t StringValueSegment = 13;
  static const uint8_t FloatValueSegment = 16;
  static const uint8_t DoubleValueSegment = 17;
  static const uint8_t LongValueSegment = 18;
  static const uint8_t BinaryValueSegment = 19;
  static const uint8_t BooleanValueSegment = 20;
};
}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
