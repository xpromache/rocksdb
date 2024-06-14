#pragma once

#include <vector>
#include <cstdint>
#include <stdexcept>
#include <rocksdb/slice.h>


namespace ROCKSDB_NAMESPACE {
namespace yamcs {

class IntValueSegment {
public:
    static const int SUBFORMAT_ID_RAW = 0;
    static const int SUBFORMAT_ID_DELTAZG_FPF128_VB = 1;
    static const int SUBFORMAT_ID_DELTAZG_VB = 2;

    IntValueSegment(bool signedVal);
       
    int getMaxSerializedSize();
    void writeTo(std::string &slice);
    static IntValueSegment parseFrom(rocksdb::Slice &slice);

private:
    bool is_signed;
    std::vector<int32_t> values;

    IntValueSegment();
    void writeCompressed(std::string &slice);
    void writeRaw(std::string &slice);
    void writeHeader(int subFormatId, std::string &slice);
    void parse(rocksdb::Slice &slice);
    void parseRaw(rocksdb::Slice &slice, int n);
    void parseCompressed(rocksdb::Slice &slice, int n, int subFormatId);
};

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
