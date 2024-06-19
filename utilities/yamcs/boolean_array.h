#pragma once

#include <algorithm>
#include <cstring>
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
      size_t idxpos = pos / 64;
      uint64_t u = a[idxpos];
      uint64_t co = u >> 63;
      uint64_t mask = -1L >> pos;
      uint64_t v = u & mask;
      u &= ~mask;
      a[idxpos] = (v << 1) | u;

      size_t idxlast = 1 + (length + 1) / 64;
      for (size_t i = idxpos + 1; i < idxlast; i++) {
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

  void push_back(uint64_t value, size_t size) {
    if (size > 64) {
      throw std::invalid_argument("Size must be between 0 and 64.");
    }
    if (size < 64) {
      // Mask to include only the relevant bits
      value = value & ((1ULL << size) - 1);
    }
    ensureCapacity(length + size);
    size_t startBit = length % 64;
    size_t idxPos = length / 64;

    if (startBit == 0) {
      // If we are at the start of a new long, just set the value
      a[idxPos] = value;
    } else {
      // Otherwise, split the value across the current long and the next one
      a[idxPos] |= value << startBit;
      if (startBit + size > 64) {
        a[idxPos + 1] = value >> (64 - startBit);
      }
    }
    length += size;
  }

  std::pair<const uint64_t*, const uint64_t*> toLongArray() const {
    // Assuming idx(length) is a valid function and length is a valid member
    const uint64_t* start = a.data();
    const uint64_t* end = a.data() + length / 64 + 1;
    return {start, end};
  }

  bool get(size_t pos) const {
    rangeCheck(pos);
    size_t idx = pos / 64;
    return (a[idx] & mask(pos)) != 0;
  }

  size_t size() const { return length; }

  void add(bool b) {
    ensureCapacity(length + 1);
    if (b) {
      set(length);
    } else {
      clear(length);
    }
    length++;
  }
  size_t length = 0;
  std::vector<uint64_t> a;

 private:
  void set(size_t pos) {
    size_t idx = pos / 64;
    a[idx] |= mask(pos);
  }

  void clear(size_t pos) {
    size_t idx = pos / 64;
    a[idx] &= ~mask(pos);
  }

  void ensureCapacity(size_t minBitCapacity) {
    size_t minCapacity = minBitCapacity / 64 + 1;
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

  void rangeCheck(size_t pos) const {
    if (pos >= length)
      throw std::out_of_range("Index: " + std::to_string(pos) +
                              " length: " + std::to_string(length));
  }

  static uint64_t mask(size_t pos) { return 1ULL << (pos % 64); }
};
