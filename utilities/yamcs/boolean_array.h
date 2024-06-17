#pragma once

#include <algorithm>
#include <cstring>
#include <span>
#include <stdexcept>
#include <vector>

class BooleanArray {
 public:
  static const int DEFAULT_CAPACITY = 5;

  BooleanArray() { a.resize(DEFAULT_CAPACITY); }

  BooleanArray(const std::vector<uint64_t>& a0, size_t length0)
      : length(length0), a(a0) {}

  void add(size_t pos, bool b) {
    if (pos > length) {
      throw std::out_of_range("Index: " + std::to_string(pos) +
                              " length: " + std::to_string(length));
    }
    ensureCapacity(length + 1);

    if (pos < length) {  // shift all bits to the right
      int idxpos = _idx(pos);
      uint64_t u = a[idxpos];
      uint64_t co = u >> 63;
      uint64_t mask = -1L >> pos;
      uint64_t v = u & mask;
      u &= ~mask;
      a[idxpos] = (v << 1) | u;

      int idxlast = 1 + _idx(length + 1);
      for (int i = idxpos + 1; i < idxlast; i++) {
        uint64_t t = a[i] >> 63;
        a[i] = co | (a[i] << 1);
        co = t;
      }
    }
    length++;
    if (b) {
      set(pos);
    } else {
      clear(pos);
    }
  }

  void push_back(long value, int size) {
    if (size < 0 || size > 64) {
      throw std::invalid_argument("Size must be between 0 and 64.");
    }

    ensureCapacity(length + size);
    int startBit = length % 64;
    int idxPos = length / 64;

    if (startBit == 0) {
      // If we are at the start of a new long, just set the value
      a[idxPos] =
          value & ((1L << size) - 1);  // Mask to include only the relevant bits

    } else {
      // Otherwise, split the value across the current long and the next one
      a[idxPos] |= (value & ((1L << size) - 1)) << startBit;
      if (startBit + size > 64) {
        a[idxPos + 1] = (value & ((1L << size) - 1)) >> (64 - startBit);
      }
    }
    length += size;
  }

  std::pair<const uint64_t*, const uint64_t*> toLongArray() const {
    // Assuming idx(length) is a valid function and length is a valid member
    const uint64_t* start = a.data();
    const uint64_t* end = a.data() + _idx(length) + 1;
    return {start, end};
  }

  bool get(int pos) const {
    rangeCheck(pos);
    int idx = _idx(pos);
    return (a[idx] & (1L << pos)) != 0;
  }

  int size() const { return length; }

  void add(bool b) {
    ensureCapacity(length + 1);
    if (b) {
      set(length);
    } else {
      clear(length);
    }
    length++;
  }

 private:
  size_t length = 0;
  std::vector<uint64_t> a;

  void set(int pos) {
    int idx = _idx(pos);
    a[idx] |= (1L << pos);
  }

  void clear(int pos) {
    int idx = _idx(pos);
    a[idx] &= ~(1L << pos);
  }

  void ensureCapacity(int minBitCapacity) {
    size_t minCapacity = _idx(minBitCapacity) + 1;
    if (minCapacity <= a.size()) {
      return;
    }

    size_t capacity = a.size();
    size_t newCapacity = capacity + (capacity >> 1);

    if (newCapacity < minCapacity) {
      newCapacity = minCapacity;
    }

    a.resize(newCapacity);
  }

  static inline int _idx(int pos) { return pos >> 6; }

  void rangeCheck(size_t pos) const {
    if (pos >= length)
      throw std::out_of_range("Index: " + std::to_string(pos) +
                              " length: " + std::to_string(length));
  }
};
