#include "boolean_value_segment.h"

#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

BooleanValueSegment::BooleanValueSegment(const Slice& slice, size_t& pos){
  MergeFrom(slice, pos);
}

void BooleanValueSegment::WriteTo(std::string& buf) {
  write_var_u32(buf, ba.size());

  auto auto [start, end] = ba.toLongArray();
  write_var_u32(buf, la.size());

  for (const uint64_t* it = start; it != end; ++it) {
    write_u64_be(*it);
  }
}

void BooleanValueSegment::MergeFrom(const rocksdb::Slice& slice, size_t& pos) {
  uint32_t size, n;
  status = read_var_u32(slice, pos, size);
  if (!status.ok()) {
    return;
  }
  status = read_var_u32(slice, pos, n);
  if (!status.ok()) {
    return;
  }

  for (int i = 0; i < n; i++) {
    uint64_t l;
    status = read_var_u64(slice, pos, l);
    if (!status.ok()) {
      return;
    }

    ba.push_back(l, size > 64 ? 64 : size);
    size -= 64;
  }
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
