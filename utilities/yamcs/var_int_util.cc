#include "var_int_util.h"
#include <stdexcept>

namespace ROCKSDB_NAMESPACE {
namespace yamcs {
size_t writeVarInt32(std::string& str, size_t pos, uint32_t x) {
    while ((x & ~0x7F) != 0) {
        if (pos >= str.size()) str.resize(str.size() + 1);
        str[pos++] = static_cast<char>((x & 0x7F) | 0x80);
        x >>= 7;
    }
    if (pos >= str.size()) str.resize(str.size() + 1);
    str[pos++] = static_cast<char>(x & 0x7F);
    return pos;
}

void writeVarInt32(std::string& str, uint32_t x) {
    while ((x & ~0x7F) != 0) {
        str.push_back(static_cast<char>((x & 0x7F) | 0x80));
        x >>= 7;
    }
    str.push_back(static_cast<char>(x & 0x7F));
}

void writeVarInt64(std::string& str, uint64_t x) {
    while ((x & ~0x7F) != 0) {
        str.push_back(static_cast<char>((x & 0x7F) | 0x80));
        x >>= 7;
    }
    str.push_back(static_cast<char>(x & 0x7F));
}

uint32_t readVarInt32(const rocksdb::Slice& slice, size_t& pos) {
    uint32_t v = 0;
    int shift = 0;
    while (shift < 35) {
        char b = slice[pos++];
        v |= (b & 0x7F) << shift;
        if (!(b & 0x80)) return v;
        shift += 7;
    }
    throw std::runtime_error("Invalid VarInt32");
}

uint64_t readVarInt64(const rocksdb::Slice& slice, size_t& pos) {
    uint64_t v = 0;
    int shift = 0;
    while (shift < 70) {
        char b = slice[pos++];
        v |= (static_cast<uint64_t>(b) & 0x7F) << shift;
        if (!(b & 0x80)) return v;
        shift += 7;
    }
    throw std::runtime_error("Invalid VarInt64");
}

void writeSignedVarint32(std::string& str, int32_t x) {
    writeVarInt32(str, encodeZigZag(x));
}

int32_t readSignedVarInt32(const rocksdb::Slice& slice, size_t& pos) {
    return decodeZigZag(readVarInt32(slice, pos));
}

size_t encodeSigned(std::string& str, size_t pos, int32_t x) {
    return writeVarInt32(str, pos, encodeZigZag(x));
}

int decodeZigZag(int x) {
    return (x >> 1) ^ -(x & 1);
}

int encodeZigZag(int x) {
    return (x << 1) ^ (x >> 31);
}

void writeSizeDelimitedString(std::string& str, const std::string& s) {
    writeVarInt32(str, s.size());
    str.append(s);
}

std::string readSizeDelimitedString(const rocksdb::Slice& slice, size_t& pos) {
    int len = readVarInt32(slice, pos);
    std::string result = slice.ToString().substr(pos, len);
    pos += len;
    return result;
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
