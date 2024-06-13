//  Copyright (c) 2024 Nicolae Mihalache.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "merge_operator.h"

#include <rocksdb/merge_operator.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/options_type.h>

#include <cassert>
#include <iostream>
#include <memory>

#include "boolean_value_segment.h"
#include "double_value_segment.h"
#include "float_value_segment.h"
#include "gap_segment.h"
#include "int_value_segment.h"
#include "logging/logging.h"
#include "long_value_segment.h"
#include "object_segment.h"
#include "util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE::yamcs {

// Implementation for the merge operation (merges two Yamcs parameter archive
// segments)
bool YamcsParchiveMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  merge_out->new_value.clear();

  if (merge_in.existing_value) {
    return MergeSlices(
        merge_in.key, *merge_in.existing_value, merge_in.operand_list.begin(),
        merge_in.operand_list.end(), &merge_out->new_value, merge_in.logger);
  } else if (!merge_in.operand_list.empty()) {
    // Use the first element of operand_list as the initial value
    const Slice& first_value = merge_in.operand_list.front();
    return MergeSlices(
        merge_in.key, first_value, merge_in.operand_list.begin() + 1,
        merge_in.operand_list.end(), &merge_out->new_value, merge_in.logger);
  } else {
    return false;
  }
}

bool YamcsParchiveMergeOperator::PartialMergeMulti(
    const Slice& key, const std::deque<Slice>& operand_list,
    std::string* new_value, Logger* logger) const {
  // Ensure the operand_list is not empty
  if (operand_list.empty()) {
    return false;
  }

  // Get the first element from the deque
  const Slice& first_value = operand_list.front();

  // Call the utility function with deque iterators
  return MergeSlices(key, first_value, operand_list.begin() + 1,
                     operand_list.end(), new_value, logger);
}

template <typename Iterator>
bool YamcsParchiveMergeOperator::MergeSlices(const Slice& key,
                                             const Slice& first_value,
                                             Iterator begin, Iterator end,
                                             std::string* new_value,
                                             Logger* logger) const {
  if (first_value.size() < 2) {
    ROCKS_LOG_ERROR(
        logger, "Short value received in the merge key: %s, value: %s",
        key.ToString(true).c_str(), first_value.ToString(true).c_str());
    return false;
  }

  uint8_t formatId = first_value.data()[0];
  size_t pos = 1;
  ROCKS_LOG_DEBUG(logger, "In MergeSlices formatId: %d", formatId);

  std::unique_ptr<ValueSegment> segment;

  switch (formatId) {
    case FID_ParameterStatusSegment:  // intentional pass through
    case FID_BinaryValueSegment:      // intentional pass through
    case FID_StringValueSegment:
      segment = std::make_unique<ObjectSegment>(first_value, pos);
      break;
    case FID_SortedTimeValueSegmentV2:  // intentional pass through
    case FID_IntValueSegment:
      segment = std::make_unique<IntValueSegment>(first_value, pos);
      break;
    case FID_FloatValueSegment:
      segment = std::make_unique<FloatValueSegment>(first_value, pos);
      break;
    case FID_DoubleValueSegment:
      segment = std::make_unique<DoubleValueSegment>(first_value, pos);
      break;
    case FID_LongValueSegment:
      segment = std::make_unique<LongValueSegment>(first_value, pos);
      break;
    case FID_BooleanValueSegment:
      segment = std::make_unique<BooleanValueSegment>(first_value, pos);
      break;
    case FID_GapSegment:
      segment = std::make_unique<GapSegment>(first_value, pos);
      break;
  }

  if (segment) {
    if (!segment->Status().ok()) {
      ROCKS_LOG_ERROR(logger,
                      "Error initializing ValueSegment for format %d. Error: "
                      "%s. when loading Key: %s Value: %s",
                      formatId, segment->Status().getState(),
                      key.ToString(true).c_str(),
                      first_value.ToString(true).c_str());
      return false;
    }

    for (auto it = begin; it != end; ++it) {
      size_t pos1 = 1;
      segment->MergeFrom(*it, pos1);
      if (!segment->Status().ok()) {
        ROCKS_LOG_ERROR(logger, "Error merging segment for format %d: %s",
                        formatId, segment->Status().getState());
        return false;
      }
    }

    new_value->push_back(static_cast<char>(formatId));
    new_value->reserve(segment->MaxSerializedSize());

    segment->WriteTo(*new_value);
    return true;
  } else {
    ROCKS_LOG_ERROR(
        logger, "Segment merger for segments with format %d not implemented",
        formatId);
    return false;
  }
}

}  // namespace ROCKSDB_NAMESPACE::yamcs
