#include "int_value_segment.h"
#include "var_int_util.h"

#include <rocksdb/slice.h>

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

IntValueSegment::IntValueSegment(bool signedVal) : is_signed(signedVal) {}

IntValueSegment::IntValueSegment() : is_signed(false) {}

void IntValueSegment::writeTo(std::string &slice) {
  const char *start = slice.data();
  try {
    writeCompressed(slice);
  } catch (const std::overflow_error &e) {
    slice.clear();
    writeRaw(slice);
  }
}

void IntValueSegment::writeCompressed(std::string &slice) {
  auto ddz = encodeDeltaDeltaZigZag(values);

  FastPFOR128 *fastpfor = FastPFOR128::get();
  int size = ddz.size();

  IntWrapper inputOffset(0);
  IntWrapper outputOffset(0);
  std::vector<int> xc(size);

  fastpfor->compress(ddz.data(), inputOffset, size, xc.data(), outputOffset);
  if (outputOffset.get() == 0) {
    writeHeader(SUBFORMAT_ID_DELTAZG_VB, slice);
  } else {
    writeHeader(SUBFORMAT_ID_DELTAZG_FPF128_VB, slice);
    int length = outputOffset.get();
    for (int i = 0; i < length; i++) {
      slice.data()[slice.size()] = xc[i];
    }
  }

  for (int i = inputOffset.get(); i < size; i++) {
    writeVarInt32(slice, ddz[i]);
  }
}

void IntValueSegment::writeRaw(std::string &slice) {
  writeHeader(SUBFORMAT_ID_RAW, slice);
  int n = values.size();
  for (int i = 0; i < n; i++) {
    slice.data()[slice.size()] = values[i];
  }
}

void IntValueSegment::writeHeader(int subFormatId, std::string &slice) {
  int x = signedValue ? 1 : 0;
  x = (x << 4) | subFormatId;
  slice.data()[slice.size()] = static_cast<uint8_t>(x);
  writeVarInt32(slice, values.size());
}

IntValueSegment IntValueSegment::parseFrom(rocksdb::Slice &slice) {
  IntValueSegment r;
  r.parse(slice);
  return r;
}

void IntValueSegment::parse(rocksdb::Slice &slice) {
  uint8_t x = slice.data()[0];
  int subFormatId = x & 0xF;
  signedValue = (((x >> 4) & 1) == 1);
  int n = VarIntUtil::readVarInt32(slice);

  switch (subFormatId) {
    case SUBFORMAT_ID_RAW:
      parseRaw(slice, n);
      break;
    case SUBFORMAT_ID_DELTAZG_FPF128_VB:
    case SUBFORMAT_ID_DELTAZG_VB:
      parseCompressed(slice, n, subFormatId);
      break;
    default:
      throw std::runtime_error("Unknown subformatId: " +
                               std::to_string(subFormatId));
  }
}

void IntValueSegment::parseRaw(rocksdb::Slice &slice, int n) {
  values.resize(n);
  for (int i = 0; i < n; i++) {
    values[i] =
        *reinterpret_cast<const int32_t *>(slice.data() + i * sizeof(int32_t));
  }
}

void IntValueSegment::parseCompressed(rocksdb::Slice &slice, int n,
                                      int subFormatId) {
  std::vector<int> ddz(n);

  IntWrapper inputOffset(0);
  IntWrapper outputOffset(0);
  size_t position = slice.data() - slice.data();

  if (subFormatId == SUBFORMAT_ID_DELTAZG_FPF128_VB) {
    std::vector<int> x((slice.size() - (slice.data() - slice.data())) / 4);
    for (int &i : x) {
      i = *reinterpret_cast<const int32_t *>(slice.data() +
                                             i * sizeof(int32_t));
    }
    FastPFOR128 *fastpfor = FastPFOR128::get();
    fastpfor->uncompress(x.data(), inputOffset, x.size(), ddz.data(),
                         outputOffset);
    slice = rocksdb::Slice(slice.data() + position + inputOffset.get() * 4);
  }

  for (int i = outputOffset.get(); i < n; i++) {
    ddz[i] = readVarInt32(slice);
  }
  values = decodeDeltaDeltaZigZag(ddz);
}

int IntValueSegment::getMaxSerializedSize() {
  return 5 + 4 * values.size();  // 1 for format id + 4 for the size plus 4 for
                                 // each element
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
