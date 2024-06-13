#include "boolean_value_segment.h"

#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

BooleanValueSegment::BooleanValueSegment(const Slice& slice, size_t& pos){
  MergeFrom(slice, pos);
}

void BooleanValueSegment::WriteTo(std::string& buf) {
  write_var_u32(buf, (uint32_t) ba.size());

  auto [start, end] = ba.toLongArray();
  write_var_u32(buf, (uint32_t) (end-start));

  for (const uint64_t* it = start; it != end; it++) {
    write_u64_be(buf, *it);
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

  if ((ba.size() + (size_t) n) > INT32_MAX) {
    status = Status::CompactionTooLarge("resulting segment would be too large");
    return;
  }
  
  if (pos + 8 * n > slice.size()) {
    status = Status::Corruption(
        "Cannot decode boolean segment: expected " + std::to_string(8 * n) +
        " bytes and only " + std::to_string(slice.size() - pos) + " available");
    return;
  }  
  for (size_t i = 0; i < n; i++) {
    uint64_t l = read_u64_be_unchecked(slice, pos);

    ba.push_back(l, size > 64 ? 64 : size);
    size -= 64;
  }
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
