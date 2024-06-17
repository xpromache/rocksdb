#include <bitset>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "bit_reader_writer.h"

/**
 * Implements the floating point compression scheme described here:
 * http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 *
 */

/**
 * compress the first n elements from the array of floats into the ByteBuffer
 */
inline void float_compress(const std::vector<float>& fa, std::string& buffer) {
  BitWriter bw(buffer);

  int xorValue;
  int prevV = *reinterpret_cast<const int*>(&fa[0]);
  bw.write(prevV, 32);

  int prevLz = 100;  // such that the first comparison lz>=prevLz will fail
  int prevTz = 0;

  auto n = fa.size();
  for (int i = 1; i < n; i++) {
    int v = *reinterpret_cast<const int*>(&fa[i]);
    xorValue = v ^ prevV;
    // If XOR with the previous is zero (same value), store single ‘0’ bit
    if (xorValue == 0) {
      bw.write(0, 1);
    } else {
      // When XOR is non-zero, calculate the number of leading and trailing
      // zeros in the XOR, store bit ‘1’ followed by either a) or b):
      bw.write(1, 1);
      int lz = __builtin_clz(xorValue);
      int tz = __builtin_ctz(xorValue);
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

rocksdb::Status float_decompress(const rocksdb::Slice& slice, size_t& pos, Vector<float>& out) {
    BitReader br(slice, pos); // Initialize BitReader with slice and pos
    int xorValue;
    int v = static_cast<int>(br.read(32));
    out.resize(1);
    out[0] = *reinterpret_cast<float*>(&v);

    int lz = 0;  // leading zeros
    int tz = 0;  // trailing zeros
    int mb = 0;  // meaningful bits

    for (size_t i = 1; i < out.size(); i++) {
        int bit = br.read(1);
        if (bit == 0) {
            // same with the previous value
            out[i] = out[i - 1];
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
            out[i] = *reinterpret_cast<float*>(&v);
        }
    }

    // Update pos to reflect the number of bytes read from the slice
    pos += br.getBytePosition();

    return rocksdb::Status::OK(); // Return OK status assuming no errors
}

static void compress(const std::vector<float>& fa, std::string& buffer) {
  compress(fa, fa.size(), buffer);
}
