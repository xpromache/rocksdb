#include "util.h"

#include <string>
#include <vector>

#include "fastpfor/fastpfor.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

thread_local FastPForLib::FastPFor<4> fastpfor128;
static thread_local std::vector<uint32_t> xc;

bool write_vec_u32_compressed(std::string& buf,
                              std::vector<uint32_t>& values_idx) {
  bool with_fastpfor;
  write_var_u32(buf, values_idx.size());
  xc.resize(3 * values_idx.size() / 2);

  size_t compressed_in_size =
      values_idx.size() / fastpfor128.BlockSize * fastpfor128.BlockSize;

  size_t compressed_out_size = xc.size();

  if (compressed_in_size > 0) {
    fastpfor128.encodeArray(values_idx.data(), values_idx.size(), xc.data(),
                            compressed_out_size);
    if (compressed_out_size < values_idx.size()) {
      with_fastpfor = true;
    }
  }

  if (compressed_in_size == 0 || compressed_out_size >= xc.size()) {
    // no compression could be done (too few data points)
    // or compression resulted in data larger than the original
    with_fastpfor = false;
    compressed_in_size = 0;
    compressed_out_size = 0;
  }
  for (size_t i = 0; i < compressed_out_size; i++) {
    write_u32_be(buf, xc[i]);
  }

  for (size_t i = compressed_in_size; i < values_idx.size(); i++) {
    write_var_u32(buf, values_idx[i]);
  }
  return with_fastpfor;
}
Status read_vec_u32_compressed(bool with_fastpfor, const rocksdb::Slice& slice,
                               size_t& pos, std::vector<uint32_t>& values) {
  uint32_t n;
  auto status = read_var_u32(slice, pos, n);
  if (!status.ok()) {
    return status;
  }

  values.resize(n);

  size_t pos1 = pos;
  size_t outputlength = 0;

  if (with_fastpfor) {
    xc.resize((slice.size() - pos) / 4);
    for (size_t i = 0; i < xc.size(); i++) {
      xc[i] = read_u32_be_unchecked(slice, pos);
    }
    outputlength = n;
    auto in2 = fastpfor128.decodeArray(xc.data(), xc.size(), values.data(),
                                       outputlength);
    if (outputlength > n) {
      return Status::Corruption("Encoded data longer than expected");
    }
    pos = pos1 + 4 * (in2 - xc.data());
  }

  for (auto i = outputlength; i < n; i++) {
    auto s = read_var_u32(slice, pos, values[i]);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}
}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE