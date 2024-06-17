#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <string>
#include <vector>


namespace ROCKSDB_NAMESPACE {
namespace yamcs {


inline int32_t decodeZigZag(uint32_t x) { return (x >> 1) ^ -(x & 1); }

inline uint32_t encodeZigZag(int32_t x) { return (x << 1) ^ (x >> 31); }

// Encode delta of deltas with ZigZag encoding
inline std::vector<uint32_t> encodeDeltaDeltaZigZag(
    const std::vector<int32_t>& x) {
  size_t n = x.size();

  std::vector<uint32_t> ddz;
  ddz.reserve(n);
  ddz.push_back(encodeZigZag(x[0]));

  int32_t d = 0;
  for (size_t i = 1; i < n; ++i) {
    int32_t d1 = x[i] - x[i - 1];
    ddz.push_back(encodeZigZag(d1 - d));
    d = d1;
  }
  return ddz;
}

// Decode delta of deltas with ZigZag decoding
inline void decodeDeltaDeltaZigZag(const std::vector<uint32_t>& ddz,
                                   std::vector<int32_t>& out) {
  size_t n = ddz.size();
  out.reserve(out.size() + n);

  int32_t first = decodeZigZag(ddz[0]);
  out.push_back(first);

  int32_t d = 0;
  for (size_t i = 1; i < n; ++i) {
    d = d + decodeZigZag(ddz[i]);
    int32_t value = out.back() + d;
    out.push_back(value);
  }
}

inline void write_var_u32(std::string& str, uint32_t x) {
  while ((x & ~0x7F) != 0) {
    str.push_back(static_cast<char>((x & 0x7F) | 0x80));
    x >>= 7;
  }
  str.push_back(static_cast<char>(x & 0x7F));
}

inline void write_var_u64(std::string& str, uint64_t x) {
  while ((x & ~0x7F) != 0) {
    str.push_back(static_cast<char>((x & 0x7F) | 0x80));
    x >>= 7;
  }
  str.push_back(static_cast<char>(x & 0x7F));
}

inline rocksdb::Status read_var_u32(const rocksdb::Slice& slice, size_t& pos,
                                    uint32_t& result) {
  result = 0;
  int shift = 0;
  while (shift < 35) {
    if (pos >= slice.size()) {
      return Status::Corruption("Buffer underflow while reading a VARINT32");
    }
    char b = slice[pos++];
    result |= (b & 0x7F) << shift;
    if (!(b & 0x80)) return Status::OK();
    shift += 7;
  }
  return Status::Corruption("Invalid VarInt32");
}

inline rocksdb::Status read_var_u64(const rocksdb::Slice& slice, size_t& pos,
                                    uint64_t& result) {
  result = 0;
  int shift = 0;
  while (shift < 70) {
    char b = slice[pos++];
    result |= (static_cast<uint64_t>(b) & 0x7F) << shift;
    if (!(b & 0x80)) return Status::OK();
    shift += 7;
  }
  return Status::Corruption("Invalid VarInt64");
}

inline void writeSignedVarint32(std::string& str, int32_t x) {
  write_var_u32(str, encodeZigZag(x));
}

inline rocksdb::Status readSignedVarInt32(const rocksdb::Slice& slice,
                                          size_t& pos, int32_t& result) {
  uint32_t v;
  Status s = read_var_u32(slice, pos, v);
  if (s.ok()) {
    result = decodeZigZag(v);
  }
  return s;
}

size_t encodeSigned(std::string& str, size_t pos, int32_t x);

void writeSizeDelimitedString(std::string& str, const std::string& s);
std::string readSizeDelimitedString(const rocksdb::Slice& slice, size_t& pos);

inline void write_u64_be(std::string& buf, uint64_t x) {
  x = htobe64(x);
  const char* data = reinterpret_cast<const char*>(&x);
  buf.append(data, sizeof(x));
}

inline void write_u32_be(std::string& buf, uint32_t x) {
  x = htobe32(x);
  const char* data = reinterpret_cast<const char*>(&x);
  buf.append(data, sizeof(x));
}

inline void write_f32_be(std::string& buf, float x) {
  x = htobe32(x);
  const char* data = reinterpret_cast<const char*>(&x);
  buf.append(data, sizeof(x));
}

inline uint32_t read_u32_be_unchecked(const rocksdb::Slice& slice,
                                      size_t& pos) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(slice.data() + pos);
  uint32_t value = 0;

  for (size_t i = 0; i < sizeof(uint32_t); ++i) {
    value = (value << 8) | data[i];
  }

  pos += sizeof(uint32_t);

  return value;
}

inline uint64_t read_u64_be_unchecked(const rocksdb::Slice& slice,
                                      size_t& pos) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(slice.data() + pos);
  uint64_t value = 0;

  for (size_t i = 0; i < sizeof(uint64_t); ++i) {
    value = (value << 8) | data[i];
  }

  pos += sizeof(uint64_t);

  return value;
}

inline rocksdb::Status read_u32_be(rocksdb::Slice& slice, size_t& pos,
                                   uint32_t& x) {
  if (pos + 4 > slice.size()) {
    return Status::Corruption("buffer underflow");
  }
  x = read_u32_be_unchecked(slice, pos);
  return Status::OK();
}
inline rocksdb::Status read_u64_be(rocksdb::Slice& slice, size_t& pos,
                                   uint64_t& x) {
  if (pos + 8 > slice.size()) {
    return Status::Corruption("buffer underflow");
  }
  x = read_u64_be_unchecked(slice, pos);
  return Status::OK();
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
