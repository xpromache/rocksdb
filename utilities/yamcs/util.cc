#include "util.h"

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {




void writeSizeDelimitedString(std::string& str, const std::string& s) {
  writeVarInt32(str, s.size());
  str.append(s);
}

Status readSizeDelimitedString(const rocksdb::Slice& slice, size_t& pos,
                               std::string& result) {
  uint32_t len;
  Status s = readVarInt32(slice, pos, len);
  if (s.ok()) {
    if (pos + len < slice.size()) {
      result.append(slice.data() + pos, len);
      pos += len;
    } else {
      s = Status::Corruption("Slice of size " + std::to_string(slice.size()) +
                             " too short to read string of length " +
                             std::to_string(len) + " at position " +
                             std::to_string(pos));
    }
  }

  return s;
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
