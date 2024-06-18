#include "object_segment.h"

#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

static void write_values(std::string& buf,
                         std::vector<std::string_view> values);

static void write_rles(std::string& buf, std::vector<uint32_t>& rle_counts,
                       std::vector<uint32_t>& rle_values);

static Status parse_values(const rocksdb::Slice& slice, size_t& pos,
                           std::vector<std::string_view>& values);
static Status parse_values_idx(int formatId, const rocksdb::Slice& slice,
                               size_t& pos, std::vector<uint32_t>& values_idx,
                               uint32_t max_idx);
static Status parse_rles(const rocksdb::Slice& slice, size_t& pos,
                         std::vector<uint32_t>& rle_counts,
                         std::vector<uint32_t>& rle_values);

ObjectSegment::ObjectSegment(const Slice& slice, size_t& pos)
    : formatId(slice.data()[pos] & 0x0f) {
  MergeFrom(slice, pos);
}

void ObjectSegment::WriteTo(std::string& buf) {
  buf.push_back(formatId);
  write_values(buf, values);
  if (formatId == SUBFORMAT_ID_ENUM_RLE) {
    write_rles(buf, rle_counts, rle_values);
  } else if (formatId == SUBFORMAT_ID_ENUM_FPROF ||
             formatId == SUBFORMAT_ID_ENUM_VB) {
    write_vec_u32_compressed(buf, values_idx);
  }
}

// TODO check the max size overflow
void ObjectSegment::MergeFrom(const rocksdb::Slice& slice, size_t& pos) {
  uint8_t x = slice.data()[pos++];

  int newSliceFid = x & 0xF;
  printf("at the MergeFrom start having %ld values\n", values.size());
  if (newSliceFid == SUBFORMAT_ID_RAW) {
    mergeRaw(slice, pos);
  } else if (newSliceFid == SUBFORMAT_ID_ENUM_RLE) {
    mergeRleEnum(slice, pos);
  } else {
    mergeNonRleEnum(newSliceFid, slice, pos);
  }

  printf("at the MergeFrom end having %ld values\n", values.size());
}

// this is called in case the chunk to be merged is in raw format
void ObjectSegment::mergeRaw(const rocksdb::Slice& slice, size_t& pos) {
  if (this->formatId == SUBFORMAT_ID_RAW) {
    // if this is also raw, then just append the values
    status = parse_values(slice, pos, values);
  } else {
    // this is one of the enum types, read the values into a temporary vector
    // and update the counters
    std::vector<std::string_view> tmp_values;
    status = parse_values(slice, pos, tmp_values);
    if (!status.ok()) {
      return;
    }
    size_t idx;
    if (this->formatId == SUBFORMAT_ID_ENUM_RLE) {
      for (auto v : tmp_values) {
        auto it = valueIndexMap.find(v);
        if (it != valueIndexMap.end()) {
          idx = it->second;
        } else {
          idx = values.size();
          values.push_back(v);
          valueIndexMap[v] = idx;
        }
        if (!rle_values.empty() && rle_values.back() == idx) {
          rle_counts.back() += 1;
        } else {
          rle_values.push_back((uint32_t) idx);
          rle_counts.push_back(1);
        }
      }
    } else {
      for (auto v : tmp_values) {
        auto it = valueIndexMap.find(v);
        if (it != valueIndexMap.end()) {
          idx = it->second;
        } else {
          idx = values.size();
          values.push_back(v);
          valueIndexMap[v] = idx;
        }
        values_idx.push_back((uint32_t)idx);
      }
    }
  }
}

// new slice is RLE enum
void ObjectSegment::mergeRleEnum(const rocksdb::Slice& slice, size_t& pos) {
  std::vector<std::string_view> tmp_values;
  status = parse_values(slice, pos, tmp_values);
  if (!status.ok()) {
    return;
  }
  if (this->formatId != SUBFORMAT_ID_ENUM_RLE) {
    rle_counts.clear();
    rle_values.clear();
  }

  auto first_rle_idx = rle_counts.size();

  status = parse_rles(slice, pos, rle_counts, rle_values);
  if (!status.ok()) {
    return;
  }
  if (this->formatId == SUBFORMAT_ID_RAW) {
    for (size_t i = 0; i < rle_counts.size(); i++) {
      auto count = rle_counts[i];
      auto idx = rle_values[i];
      if (idx >= tmp_values.size()) {
        status = Status::Corruption(
            "Enum RLE value refers to inexistent value with index " +
            std::to_string(idx));
        return;
      }
      for (size_t j = 0; j < count; j++) {
        values.push_back(tmp_values[idx]);
      }
    }
  } else if (this->formatId == SUBFORMAT_ID_ENUM_RLE) {
    // merging RLE enum to RLE enum,
    // add all the unique values to the map and adjust the new rle_values with
    // the new indices rle_counts stay the same
    auto mappings = AddEnumValues(tmp_values);
    for (auto i = first_rle_idx; i < rle_counts.size(); i++) {
      auto idx = rle_values[i];
      if (idx > tmp_values.size()) {
        status = Status::Corruption(
            "Enum RLE value refers to inexistent value with index " +
            std::to_string(idx));
        return;
      }
      rle_values[i] = mappings[idx];
    }
  } else {
    auto mappings = AddEnumValues(tmp_values);
    /// merging RLE enum to non RLE enum
    for (size_t i = 0; i < rle_counts.size(); i++) {
      auto count = rle_counts[i];
      auto idx = rle_values[i];
      if (idx >= tmp_values.size()) {
        status = Status::Corruption(
            "Enum RLE value refers to inexistent value with index " +
            std::to_string(idx));
        return;
      }
      for (size_t j = 0; j < count; j++) {
        values_idx.push_back(mappings[idx]);
      }
    }
  }
}

// new slice is non RLE enum
void ObjectSegment::mergeNonRleEnum(int newSliceFid,
                                    const rocksdb::Slice& slice, size_t& pos) {
  std::vector<std::string_view> tmp_values;
  status = parse_values(slice, pos, tmp_values);
  if (!status.ok()) {
    return;
  }
  std::vector<uint32_t> tmp_values_idx;
  status = parse_values_idx(newSliceFid, slice, pos, tmp_values_idx,
                            tmp_values.size());
  if (!status.ok()) {
    return;
  }

  if (this->formatId == SUBFORMAT_ID_RAW) {
    printf(" aici object store 100 tmp_values_idx.size: %ld values.size: %ld\n",
           tmp_values_idx.size(), values.size());
    for (auto idx : tmp_values_idx) {
      values.push_back(tmp_values[idx]);
    }
    printf(" aici object store 101 tmp_values_idx.size: %ld values.size: %ld\n",
           tmp_values_idx.size(), values.size());

  } else if (this->formatId == SUBFORMAT_ID_ENUM_RLE) {
    printf(" aici object store 200\n");
    // merge nonRLE enum to RLE enum
    auto mappings = AddEnumValues(tmp_values);
    for (auto idx : tmp_values_idx) {
      auto mapped_idx = mappings[idx];
      if (!rle_values.empty() && rle_values.back() == mapped_idx) {
        rle_counts.back() += 1;
      } else {
        rle_values.push_back((uint32_t) mapped_idx);
        rle_counts.push_back(1);
      }
    }
  } else {
    printf(" aici object store 300\n");
    // merge nonRLE enum to non RLE enum
    auto mappings = AddEnumValues(tmp_values);
    for (auto idx : tmp_values_idx) {
      values_idx.push_back(mappings[idx]);
    }
  }
}

// add new values to the values and valueIndexMap and return mapping from the
// chunk idx to the overall idx
std::unordered_map<size_t, size_t> ObjectSegment::AddEnumValues(
    const std::vector<std::string_view>& tmp_values) {
  std::unordered_map<size_t, size_t> mappings;
  for (size_t i = 0; i < tmp_values.size(); i++) {
    auto v = tmp_values[i];
    auto it = valueIndexMap.find(v);
    size_t idx;
    if (it != valueIndexMap.end()) {
      idx = it->second;
    } else {
      idx = values.size();
      values.emplace_back(v);
      valueIndexMap[values.back()] = idx;
    }
    mappings[i] = idx;
  }
  return mappings;
}

// reads all the values from the Slice
static Status parse_values(const rocksdb::Slice& slice, size_t& pos,
                           std::vector<std::string_view>& values) {
  uint32_t count;
  auto status = read_var_u32(slice, pos, count);
  if (!status.ok()) {
    return status;
  }

  values.reserve(count);
  for (size_t i = 0; i < count; i++) {
    uint32_t str_len;

    status = read_var_u32(slice, pos, str_len);
    if (!status.ok()) {
      return status;
    }

    if (pos + str_len > slice.size()) {
      status = Status::Corruption(
          "Cannot decode object segment: expected " +
          std::to_string(pos + str_len) + " bytes and only " +
          std::to_string(slice.size() - pos) + " available");
      return status;
    }

    values.push_back(std::string_view(slice.data() + pos, str_len));
    pos += str_len;
  }
  return Status::OK();
}

static void write_values(std::string& buf,
                         std::vector<std::string_view> values) {
  write_var_u32(buf, (uint32_t) values.size());
  for (auto v : values) {
    write_var_u32(buf, (uint32_t) v.size());
    buf.append(v);
  }
}

static Status parse_rles(const rocksdb::Slice& slice, size_t& pos,
                         std::vector<uint32_t>& rle_counts,
                         std::vector<uint32_t>& rle_values) {
  uint32_t count;
  auto status = read_var_u32(slice, pos, count);
  if (!status.ok()) {
    return status;
  }
  for (size_t i = 0; i < count; i++) {
    uint32_t v;
    status = read_var_u32(slice, pos, v);
    if (!status.ok()) {
      return status;
    }
    rle_counts.push_back(v);
  }
  for (size_t i = 0; i < count; i++) {
    uint32_t v;
    status = read_var_u32(slice, pos, v);
    if (!status.ok()) {
      return status;
    }
    rle_values.push_back(v);
  }
  return Status::OK();
}

static void write_rles(std::string& buf, std::vector<uint32_t>& rle_counts,
                       std::vector<uint32_t>& rle_values) {
  assert(rle_counts.size() == rle_values.size());
  write_var_u32(buf, (uint32_t) rle_counts.size());
  for (auto v : rle_counts) {
    write_var_u32(buf, v);
  }
  for (auto v : rle_values) {
    write_var_u32(buf, v);
  }
}

static Status parse_values_idx(int formatId, const rocksdb::Slice& slice,
                               size_t& pos, std::vector<uint32_t>& values_idx,
                               uint32_t max_idx) {
  auto status = read_vec_u32_compressed(
      formatId == ObjectSegment::SUBFORMAT_ID_ENUM_FPROF, slice, pos,
      values_idx);

  if (!status.ok()) {
    return status;
  }

  printf("in ObjectSegment parse_values_idx, parsed: %ld values\n",
         values_idx.size());

  for (auto v : values_idx) {
    // check that the indices are not out of range (with respect to the values
    // that have been previously read from the slice)
    if (v >= max_idx) {
      return Status::Corruption("Enum idx " + std::to_string(v) +
                                " larger than the maximum number of values " +
                                std::to_string(max_idx));
    }
  }
  printf("bumbalumba\n");
  return Status::OK();
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
