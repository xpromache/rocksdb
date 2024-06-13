/**
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 * (c) Daniel Lemire, http://lemire.me/en/
 */

#ifndef CODECS_H_
#define CODECS_H_

#include "common.h"
#include "util.h"
#include "bitpackinghelpers.h"

namespace FastPForLib {

class NotEnoughStorage : public std::runtime_error {
public:
  size_t required; // number of 32-bit symbols required
  NotEnoughStorage(const size_t req)
      : runtime_error(""), required(req) {}
};

class IntegerCODEC {
public:
  /**
   * You specify input and input length, as well as
   * output and output length. nvalue gets modified to
   * reflect how much was used. If the new value of
   * nvalue is more than the original value, we can
   * consider this a buffer overrun.
   *
   * You are responsible for allocating the memory (length
   * for *in and nvalue for *out).
   */
  virtual void encodeArray(const uint32_t *in, const size_t length,
                           uint32_t *out, size_t &nvalue) = 0;

  virtual void encodeArray(const uint64_t *, const size_t ,
                           uint32_t *, size_t &) {
    throw std::logic_error("Not implemented!");
  }
  /**
   * Usage is similar to decodeArray except that it returns a pointer
   * incremented from in. In theory it should be in+length. If the
   * returned pointer is less than in+length, then this generally means
   * that the decompression is not finished (some scheme compress
   * the bulk of the data one way, and they then they compress remaining
   * integers using another scheme).
   *
   * As with encodeArray, you need to have length element allocated
   * for *in and at least nvalue elements allocated for out. The value
   * of the variable nvalue gets updated with the number actually use
   * (if nvalue exceeds the original value, there might be a buffer
   * overrun).
   */
  virtual const uint32_t *decodeArray(const uint32_t *in, const size_t length,
                                      uint32_t *out, size_t &nvalue) = 0;

  virtual const uint32_t *decodeArray(const uint32_t *, const size_t ,
                                      uint64_t *, size_t &) {
    throw std::logic_error("Not implemented!");
  }

  virtual ~IntegerCODEC() {}

  /**
   * Will compress the content of a vector into
   * another vector.
   *
   * This is offered for convenience. It might be slow.
   */
  virtual std::vector<uint32_t> compress(const std::vector<uint32_t> &data) {
    std::vector<uint32_t> compresseddata(data.size() * 2 +
                                         1024); // allocate plenty of memory
    size_t memavailable = compresseddata.size();
    encodeArray(&data[0], data.size(), &compresseddata[0], memavailable);
    compresseddata.resize(memavailable);
    return compresseddata;
  }

  /**
   * Will uncompress the content of a vector into
   * another vector. Some CODECs know exactly how much data to uncompress,
   * others need to uncompress it all to know how data there is to uncompress...
   * So it useful to have a hint (expected_uncompressed_size) that tells how
   * much data there will be to uncompress. Otherwise, the code will
   * try to guess, but the result is uncertain and inefficient. You really
   * ought to keep track of how many symbols you had compressed.
   *
   * For convenience. Might be slow.
   */
  virtual std::vector<uint32_t>
  uncompress(const std::vector<uint32_t> &compresseddata,
             size_t expected_uncompressed_size = 0) {
    std::vector<uint32_t> data(
        expected_uncompressed_size); // allocate plenty of memory
    size_t memavailable = data.size();
    try {
      decodeArray(&compresseddata[0], compresseddata.size(), &data[0],
                  memavailable);
    } catch (NotEnoughStorage &nes) {
      data.resize(nes.required + 1024);
      decodeArray(&compresseddata[0], compresseddata.size(), &data[0],
                  memavailable);
    }
    data.resize(memavailable);
    return data;
  }

  virtual std::string name() const = 0;
};


} // namespace FastPForLib

#endif /* CODECS_H_ */
