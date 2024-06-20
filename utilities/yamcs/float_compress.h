#include <bitset>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "bit_reader_writer.h"
#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {
/**
 * Implements the floating point compression scheme described here:
 * http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 *
 */
/**
 * compress the floats into the buffer
 */
inline void float_compress(const std::vector<float>& fa, std::string& buffer) {
  BitWriter bw(buffer);

  uint32_t xorValue;
  uint32_t prevV = f32_to_u32_bits(fa[0]);
  bw.write(prevV, 32);

  int prevLz = 100;  // such that the first comparison lz>=prevLz will fail
  int prevTz = 0;

  auto n = fa.size();
  for (size_t i = 1; i < n; i++) {
    uint32_t v = f32_to_u32_bits(fa[i]);
    xorValue = v ^ prevV;
    // If XOR with the previous is zero (same value), store single ‘0’ bit
    if (xorValue == 0) {
      bw.write(0, 1);
    } else {
      // When XOR is non-zero, calculate the number of leading and trailing
      // zeros in the XOR, store bit ‘1’ followed by either a) or b):
      bw.write(1, 1);
      int lz = clz(xorValue);
      int tz = ctz(xorValue);
      if ((lz >= prevLz) && (tz >= prevTz) && (lz < prevLz + 7)) {
        // (a) (Control bit ‘0’) If the block of meaningful bits falls within
        // the block of previous meaningful bits, i.e., there are at least as
        // many leading zeros and as many trailing zeros as with the previous
        // value, use that information for the block position and just store the
        // meaningful XORed value.
        bw.write(0, 1);
        bw.write(xorValue >> prevTz, 32 - prevLz - prevTz);
      } else {
        // (b) (Control bit ‘1’) Store the length of the number of leading zeros
        // in the next 5 bits, then store the length of the meaningful XORed
        // value in the next 6 bits. Finally store the meaningful bits of the
        // XORed value.
        int mb = 32 - lz - tz;  // meaningful bits

        bw.write(1, 1);
        bw.write(lz, 5);
        bw.write(mb, 5);
        bw.write(xorValue >> tz, mb);
        prevLz = lz;
        prevTz = tz;
      }
    }
    prevV = v;
  }
  bw.flush();
}

/// decompress n floats from slice at the given position into the out vector.
/// The pos is updated with the bytes read. if there is an error the status
/// return will not not ok and the position will be undefined.
rocksdb::Status float_decompress(const rocksdb::Slice& slice, size_t& pos,
                                 const size_t n, std::vector<float>& out) {
  out.reserve(out.size() + n);
  BitReader br((const unsigned char*)(slice.data() + pos),
               slice.size() - pos);  // Initialize BitReader with slice and pos
  int xorValue;
  uint32_t v = br.read(32);

  out.push_back(u32_bits_to_f32(v));

  int lz = 0;  // leading zeros
  int tz = 0;  // trailing zeros
  int mb = 0;  // meaningful bits

  for (size_t i = 1; i < n; i++) {
    int bit = br.read(1);
    if (bit == 0) {
      // same with the previous value
      out.push_back(out.back());
    } else {
      bit = br.read(1);
      if (bit == 0) {
        // the block of meaningful bits falls within the block of previous
        // meaningful bits,
        xorValue = br.read(mb) << tz;
        v = xorValue ^ v;
      } else {
        lz = br.read(5);
        mb = br.read(5);
        // this happens when mb is 32 and overflows the 5 bits
        if (mb == 0) mb = 32;
        tz = 32 - lz - mb;
        xorValue = br.read(mb) << tz;
        v = xorValue ^ v;
      }
      out.push_back(u32_bits_to_f32(v));
    }
  }

  pos += br.getBytePosition();

  return rocksdb::Status::OK();
}


}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE