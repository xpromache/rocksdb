#include "int_value_segment.h"

#include <rocksdb/slice.h>

#include "fastpfor/fastpfor.h"
#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

static thread_local FastPForLib::FastPFor<4> fastpfor128;
static thread_local std::vector<uint32_t> xc;

bool get_signed(uint8_t x) { return (((x >> 4) & 1) == 1); }

IntValueSegment::IntValueSegment(const Slice &slice, size_t &pos)
    : is_signed(get_signed(slice.data()[pos])) {
  MergeFrom(slice, pos);
}

void IntValueSegment::WriteTo(std::string &buf) {
  size_t buf_pos = buf.size();
  writeCompressed(buf);
  if (buf.size() > buf_pos + values.size() * 4) {
    buf.resize(buf_pos);
    writeRaw(buf);
  }
}
void IntValueSegment::writeCompressed(std::string &buf) {
  std::vector<uint32_t> ddz = encodeDeltaDeltaZigZag(values);
  xc.resize(3 * ddz.size()/2);

  size_t compressed_in_size =
      ddz.size() / fastpfor128.BlockSize * fastpfor128.BlockSize;

  size_t compressed_out_size = 2 * ddz.size();

  if (compressed_in_size > 0) {
    fastpfor128.encodeArray(ddz.data(), ddz.size(), xc.data(),
                            compressed_out_size);
    if (compressed_out_size < ddz.size()) {
      writeHeader(SUBFORMAT_ID_DELTAZG_FPF128_VB, buf);
    }
  }

  if (compressed_in_size == 0 || compressed_out_size >= xc.size()) {
    // no compression could be done (too few data points)
    // or compression resulted in data larger than the original
    writeHeader(SUBFORMAT_ID_DELTAZG_VB, buf);
    compressed_in_size = 0;
    compressed_out_size = 0;
  }
  for (size_t i = 0; i < compressed_out_size; i++) {
    write_u32_be(buf, xc[i]);
  }

  for (size_t i = compressed_in_size; i < ddz.size(); i++) {
    writeVarInt32(buf, ddz[i]);
  }
}

void IntValueSegment::writeRaw(std::string &buf) {
  writeHeader(SUBFORMAT_ID_RAW, buf);
  int n = values.size();
  for (int i = 0; i < n; i++) {
    write_u32_be(buf, values[i]);
  }
}

void IntValueSegment::writeHeader(int subFormatId, std::string &buf) {
  int x = is_signed ? 1 : 0;
  x = (x << 4) | subFormatId;
  buf.push_back(static_cast<uint8_t>(x));
  writeVarInt32(buf, values.size());
}

void IntValueSegment::MergeFrom(const rocksdb::Slice &slice,
                                           size_t &pos) {
  uint8_t x = slice.data()[pos++];
  int subFormatId = x & 0xF;
  if (is_signed != get_signed(x)) {
    if (is_signed) {
      status = Status::Corruption(
          "Expected signed data but the merge contains unsigned");
    } else {
      status = Status::Corruption(
          "Expected unsigned data but the merge contains signed");
    }
    return;
  }

  uint32_t n;
  status = readVarInt32(slice, pos, n);
  if (!status.ok()) {
    return;
  }

  if (n == 0) {
    status = Status::Corruption("Encountered segment with 0 elements");
    return;
  }

  switch (subFormatId) {
    case SUBFORMAT_ID_RAW:
      status = parseRaw(slice, pos, n);
      break;
    case SUBFORMAT_ID_DELTAZG_FPF128_VB:
    case SUBFORMAT_ID_DELTAZG_VB:
      status = parseCompressed(slice, pos, n, subFormatId);
      break;
    default:
      status = Status::Corruption("Unknown subformatId: " +
                                std::to_string(subFormatId));
  }
}

rocksdb::Status IntValueSegment::parseRaw(const rocksdb::Slice &slice,
                                          size_t &pos, size_t n) {
  if (pos + 4 * n > slice.size()) {
    return Status::Corruption(
        "Cannot decode long segment: expected " + std::to_string(4 * n) +
        " bytes and only " + std::to_string(slice.size() - pos) + " available");
  }

  values.reserve(values.size() + n);
  for (size_t i = 0; i < n; i++) {
    values.push_back(read_u32_be_unchecked(slice, pos));
  }

  return Status::OK();
}

// n is the number of values to be read
rocksdb::Status IntValueSegment::parseCompressed(const rocksdb::Slice &slice,
                                                 size_t &pos, size_t n,
                                                 int subFormatId) {
  std::vector<uint32_t> ddz(n);

  size_t pos1 = pos;
  size_t outputlength = 0;

  if (subFormatId == SUBFORMAT_ID_DELTAZG_FPF128_VB) {
    xc.resize((slice.size() - pos) / 4);
    for (size_t i = 0; i < xc.size(); i++) {
      xc[i] = read_u32_be_unchecked(slice, pos);
    }
    outputlength = n;
    auto in2 =
        fastpfor128.decodeArray(xc.data(), xc.size(), ddz.data(), outputlength);
    if (outputlength > n) {
      return Status::Corruption("Encoded data longer than expected");
    }
    pos = pos1 + 4 * (in2 - xc.data());
  }

  for (auto i = outputlength; i < n; i++) {
    auto s = readVarInt32(slice, pos, ddz[i]);
    if (!s.ok()) {
      return s;
    }
  }

  decodeDeltaDeltaZigZag(ddz, values);

  return Status::OK();
}

size_t IntValueSegment::MaxSerializedSize() {
  return 5 + 4 * values.size();  // 1 for format id + 4 for the size plus 4 for
                                 // each element
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
