#pragma once

#include <string>
#include <rocksdb/slice.h>

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

size_t writeVarInt32(std::string& str, size_t pos, uint32_t x);
void writeVarInt32(std::string& str, uint32_t x);
void writeVarInt64(std::string& str, uint64_t x);
uint32_t readVarInt32(const rocksdb::Slice& slice, size_t& pos);
uint64_t readVarInt64(const rocksdb::Slice& slice, size_t& pos);
void writeSignedVarint32(std::string& str, int32_t x);
int32_t readSignedVarInt32(const rocksdb::Slice& slice, size_t& pos);
size_t encodeSigned(std::string& str, size_t pos, int32_t x);
int decodeZigZag(int x);
int encodeZigZag(int x);
void writeSizeDelimitedString(std::string& str, const std::string& s);
std::string readSizeDelimitedString(const rocksdb::Slice& slice, size_t& pos);

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE

