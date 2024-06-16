//  Copyright (c) 2024 Nicolae Mihalache.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "merge_operator.h"

#include <cassert>
#include <iostream>
#include <memory>

#include "int_value_segment.h"
#include "logging/logging.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_type.h"
#include "util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE::yamcs {

// Implementation for the merge operation (merges two Yamcs parameter archive
// segments)
bool YamcsParchiveMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  printf("In FullMergeV2 new_value\n");

  merge_out->new_value.clear();

  if (merge_in.existing_value) {
    return MergeSlices(
        merge_in.key, *merge_in.existing_value, merge_in.operand_list.begin(),
        merge_in.operand_list.end(), &merge_out->new_value, merge_in.logger);
  } else {
    return false;  // Or handle the error as appropriate
  }
}

bool YamcsParchiveMergeOperator::PartialMergeMulti(
    const Slice& key, const std::deque<Slice>& operand_list,
    std::string* new_value, Logger* logger) const {
  std::cout << "In PartialMergeMulti\n";

  // Ensure the operand_list is not empty
  if (operand_list.empty()) {
    return false;  // Or handle the error as appropriate
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
  ROCKS_LOG_WARN(logger, "In MergeSlices formatId: %d", formatId);

  switch (formatId) {
    case FID_SortedTimeValueSegment:
      break;
    case FID_ParameterStatusSegment:
      break;
    case FID_IntValueSegment: {
      IntValueSegment ivs(first_value, pos);
      if (!ivs.Status().ok()) {
        ROCKS_LOG_ERROR(logger, "Error initializing IntValueSegment: %s", ivs.Status().getState());
        return false;
      }

      for (auto it = begin; it != end; ++it) {
        size_t pos1 = 1;
        ivs.MergeFrom(*it, pos1);
        if (!ivs.Status().ok()) {
          ROCKS_LOG_ERROR(logger, "Error merging segment: %s", ivs.Status().getState());
          return false;
        }
      }
      new_value->push_back(static_cast<char>(formatId));

      ivs.WriteTo(*new_value);
      break;
    }
    case FID_StringValueSegment:
      break;
    case FID_FloatValueSegment:
      break;
    case FID_DoubleValueSegment:
      break;
    case FID_LongValueSegment:
      break;
    case FID_BinaryValueSegment:
      break;
    case FID_BooleanValueSegment:
      break;
  }

  return true;
}

}  // namespace ROCKSDB_NAMESPACE::yamcs
