#pragma once

#include <string>
#include <vector>

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include "portable_endian.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {


inline int32_t decodeZigZag(uint32_t x) {
    return (x >> 1) ^ static_cast<int32_t>(-(static_cast<int32_t>(x & 1)));
}

inline uint32_t encodeZigZag(int32_t x) { return (x << 1) ^ (x >> 31); }

inline float u32_bits_to_f32(uint32_t bits) {
  float value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

inline double u64_bits_to_f64(uint64_t bits) {
  double value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

static uint32_t f32_to_u32_bits(float value) {
  uint32_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

static uint64_t f64_to_u64_bits(double value) {
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

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
  uint32_t data = htobe32(f32_to_u32_bits(x));

  buf.append(reinterpret_cast<const char*>(&data), sizeof(data));
}

inline void write_f64_be(std::string& buf, double x) {
  uint64_t data = htobe64(f64_to_u64_bits(x));

  buf.append(reinterpret_cast<const char*>(&data), sizeof(data));
}

inline uint32_t read_u32_be_unchecked(const rocksdb::Slice& slice,
                                      size_t& pos) {
  const uint32_t* data = reinterpret_cast<const uint32_t*>(slice.data() + pos);
  uint32_t value = be32toh(*data);

  pos += sizeof(uint32_t);

  return value;
}

inline uint64_t read_u64_be_unchecked(const rocksdb::Slice& slice,
                                      size_t& pos) {
  const uint64_t* data = reinterpret_cast<const uint64_t*>(slice.data() + pos);
  uint64_t value = be64toh(*data);

  pos += sizeof(uint64_t);

  return value;
}

inline double read_f64_be_unchecked(const rocksdb::Slice& slice, size_t& pos) {
  uint64_t data;
  std::memcpy(&data, slice.data() + pos, sizeof(data));

  double value = u64_bits_to_f64(be64toh(data));
  pos += sizeof(uint64_t);

  return value;
}

inline float read_f32_be_unchecked(const rocksdb::Slice& slice, size_t& pos) {
  uint32_t data;
  std::memcpy(&data, slice.data() + pos, sizeof(data));
  float value = u32_bits_to_f32(be32toh(data));
  pos += sizeof(uint32_t);

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

bool write_vec_u32_compressed(std::string& buf,
                              std::vector<uint32_t>& values_idx);

Status read_vec_u32_compressed(bool with_fprof, const rocksdb::Slice& slice,
                               size_t& pos, std::vector<uint32_t>& values_idx);


#ifdef _MSC_VER
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanForward)

inline int clz(uint32_t x) {
    unsigned long index;
    _BitScanReverse(&index, x);
    return 31 - index;
}

inline int ctz(uint32_t x) {
    unsigned long index;
    _BitScanForward(&index, x);
    return index;
}
#else
#define clz(x) __builtin_clz(x)
#define ctz(x) __builtin_ctz(x)
#endif

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
