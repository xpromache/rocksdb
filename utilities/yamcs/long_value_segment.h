#pragma once

#include <vector>
#include <cstdint>
#include <stdexcept>
#include <rocksdb/slice.h>


namespace ROCKSDB_NAMESPACE {
namespace yamcs {

class LongValueSegment  {
public:
    enum Type {
        UINT64,
        SINT64,
        TIMESTAMP,
        
    };
    static const int SUBFORMAT_ID_RAW = 0;

    LongValueSegment(Type type);
    void writeTo(rocksdb::Slice& slice);
    int getMaxSerializedSize();
    
    static LongValueSegment parseFrom(rocksdb::Slice& slice);

private:
    LongValueSegment();
    void writeHeader(int subFormatId, rocksdb::Slice& slice);
    void parse(rocksdb::Slice& slice);
    void parseRaw(rocksdb::Slice& slice, int n);
    int getNumericType(Type type);

    std::vector<int64_t> values;
    int numericType; // index in the array
    static const Type types[];
};

} // namespace yamcs
} // namespace ROCKSDB_NAMESPACE

