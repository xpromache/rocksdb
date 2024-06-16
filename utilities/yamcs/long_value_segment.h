#pragma once

#include <vector>
#include <cstdint>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

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
    static const int SUBFORMAT_ID_RAW = 0;

    LongValueSegment(Type type);
    void WriteTo(std::string& slice);
    int getMaxSerializedSize();
    rocksdb::Status MergeFrom(const rocksdb::Slice& slice, size_t& pos);
    
private:
    LongValueSegment();
    void writeHeader(int subFormatId, std::string& slice);
    void parseRaw(const rocksdb::Slice& slice, int n);
    int getNumericType(Type type);

    std::vector<int64_t> values;
    int numericType; // index in the array
};

} // namespace yamcs
} // namespace ROCKSDB_NAMESPACE

