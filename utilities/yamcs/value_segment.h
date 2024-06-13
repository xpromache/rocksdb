
#pragma once

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

#include <rocksdb/slice.h>
#include <rocksdb/status.h>

class ValueSegment {
 public:
  virtual ~ValueSegment() =
      default;

  virtual void WriteTo(std::string &slice) = 0;
  virtual void MergeFrom(const rocksdb::Slice &slice, size_t &pos) = 0;
  virtual size_t MaxSerializedSize() = 0;

  rocksdb::Status& Status() {
    return status;
  }
  
  protected:
  rocksdb::Status status;
    
};
}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE