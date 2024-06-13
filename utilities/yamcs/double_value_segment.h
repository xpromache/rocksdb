#pragma once

#include <vector>
#include <cstdint>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include "value_segment.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

class DoubleValueSegment : public ValueSegment {
public:
    static const uint8_t SUBFORMAT_ID_RAW = 0;

    DoubleValueSegment(const Slice &slice, size_t &pos);
    
    void WriteTo(std::string& slice);
    void MergeFrom(const rocksdb::Slice& slice, size_t& pos);
    size_t MaxSerializedSize() {
         return 4 + 8 * values.size();
    }

private:
    void parseRaw(const rocksdb::Slice& slice, int n);
    std::vector<double> values;
};

} // namespace yamcs
} // namespace ROCKSDB_NAMESPACE

