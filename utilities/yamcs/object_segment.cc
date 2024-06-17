#include "object_segment.h"

#include "util.h"

namespace ROCKSDB_NAMESPACE {
namespace yamcs {

Status parse_values(const int n, const rocksdb::Slice& slice, size_t& pos,
                    Vec<std::string_view>& values);


ObjectSegment::ObjectSegment(const Slice &slice, size_t &pos)
    : formatId(slice.data()[pos] & 0x0f) {
  MergeFrom(slice, pos);
}

void ObjectSegment::WriteTo(std::string& buf) {
  buf.push_back(formatId);
  write_values(buf, values);
  if (formatId == SUBFORMAT_ID_ENUM_RLE) {
    write_rles(buf, rle_counts, rle_values);
  }
}

void ObjectSegment::MergeFrom(const rocksdb::Slice& slice, size_t& pos) {
  uint8_t x = slice.data()[pos++];

  int newSliceFid = x & 0xF;

  if (newSliceFid == SUBFORMAT_ID_RAW) {
    mergeRaw(slice, pos);
  } else if (newSliceFid == SUBFORMAT_ID_ENUM_RLE) {
    mergeRleEnum(slice, pos);
  } else {
    mergeNonRleEnum(newSliceFid, slice, pos);
  }
}

// this is called in case the chunk to be merged is in raw format
void ObjectSegment::mergeRaw(const rocksdb::Slice& slice, size_t& pos) {
  if (this->formatId == SUBFORMAT_ID_RAW) {
    // if this is also raw, then just append the values
    status = parse_values(slice, pos, values);
  } else {
    // this is one of the enum types, read the values into a temporary vector
    // and update the counters
    Vec<std::string_view> tmp_values;
    status = parse_values(slice, pos, tmp_values);
    if (!status.ok()) {
      return;
    }
    if (this->formatId == SUBFORMAT_ID_ENUM_RLE) {
      for (v : tmp_values) {
        auto it = valueIndexMap.find(v);
        if (it != valueIndexMap.end()) {
          idx = it->second;
        } else {
          idx = values.size();
          valueIndexMap[value] = idx;
        }
        if (!rle_values.is_empty() && rle_values.back() == idx) {
          rle_counts.back() += 1;
        } else {
          rle_values.push_back(idx);
          rle_counts.push_back(1);
        }
      }
    } else {
      for (v : tmp_values) {
        auto it = valueIndexMap.find(v);
        if (it != valueIndexMap.end()) {
          idx = it->second;
        } else {
          idx = values.size();
          valueIndexMap[value] = idx;
        }
        values_idx.push_back(idx);
      }
    }
  }
}

// new slice is RLE enum
void ObjectSegment::mergeRleEnum(const rocksdb::Slice& slice, size_t& pos) {
  Vec<std::string_view> tmp_values;
  status = parse_values(slice, pos, tmp_values);
  if (!status.ok()) {
    return;
  }
  if (this->subFormatId != SUBFORMAT_ID_ENUM_RLE) {
    rle_counts.clear();
    rle_values.clear();
  }

  auto first_rle_idx = rle_counts.size();

  status = parse_rles(slice, pos, rle_counts, rle_values);
  if (!status.ok()) {
    return;
  }
  if (this->subFormatId == SUBFORMAT_ID_RAW) {
    for (int i = 0; i < rle_counts.size(); i++) {
      auto count = rle_counts[i];
      auto idx = rle_values[i];
      if (idx >= tmp_values.size()) {
        status = Status::Corruption(
            "Enum RLE value refers to inexistent value with index " +
            std::to_string(idx));
        return;
      }
      values.push_back(tmp_values[idx]);
    }
  } else if (this->subFormatId == SUBFORMAT_ID_ENUM_RLE) {
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
    for (int i = 0; i < rle_counts.size(); i++) {
      auto count = rle_counts[i];
      auto idx = rle_values[i];
      if (idx >= tmp_values.size()) {
        status = Status::Corruption(
            "Enum RLE value refers to inexistent value with index " +
            std::to_string(idx));
        return;
      }
      for (int j = 0; j < count; j++) {
        values_idx.push_back(mappings[idx]);
      }
    }
  }
}

// new slice is non RLE enum
void ObjectSegment::mergeNonRleEnum(int newSliceFid,
                                    const rocksdb::Slice& slice, size_t& pos) {
  Vec<std::string_view> tmp_values;
  status = parse_values(slice, pos, tmp_values);
  if (!status.ok()) {
    return;
  }
  Vec<uint32_t> tmp_values_idx;
  status ==
      parse_values_idx(newSliceFid, slice, pos, values_idx, tmp_values.size());
  if (!status.ok()) {
    return;
  }

  if (this->formatId == SUBFORMAT_ID_RAW) {
    for (auto idx : tmp_values_idx) {
      values.push_back(tmp_values[idx]);
    }
  } else if (this->formatId == SUBFORMAT_ID_ENUM_RLE) {
    // merge nonRLE enum to RLE enum
    auto mappings = AddEnumValues(tmp_values);
    for (auto idx : tmp_values_idx) {
      auto mapped_idx = mappings[idx];
      if (!rle_values.is_empty() && rle_values.back() == mapped_idx) {
        rle_counts.back() += 1;
      } else {
        rle_values.push_back(mapped_idx);
        rle_counts.push_back(1);
      }
    }
  } else {
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
Status parse_values(const rocksdb::Slice& slice, size_t& pos,
                    Vec<std::string_view>& values) {
  uint32_t count;
  auto status = read_var_u32(slice, pos, count);
  if (!status.ok()) {
    return status;
  }

  if (pos + 8 * count > slice.size()) {
    status = Status::Corruption(
        "Cannot decode long segment: expected " + std::to_string(8 * count) +
        " bytes and only " + std::to_string(slice.size() - pos) + " available");
    return;
  }
  values.reserve(count);
  for (size_t i = 0; i < count; i++) {
    uint32_t str_len;

    auto status = read_var_u32(slice, pos, str_len);
    if (!status.ok()) {
      return status;
    }

    if (pos + str_len > size) {
      status = Status::Corruption(
          "Cannot decode segment: expected " + std::to_string(pos + str_len) +
          " bytes and only " + std::to_string(size - pos) + " available");
      return status;
    }

    std::string_view value(data + pos, str_len);
    new_values.push_back(value);

    return Status::OK();
  }
}

void write_values(std::string& buf, Vec<std::string_view> values) {
  write_var_u32(buf, values.size());
  for (v : values) {
    write_var_u32(buf, v.size);
    buf.append(v);
  }
}

Status parse_rles(const rocksdb::Slice& slice, size_t& pos,
                  Vec<uint32_t>& rle_counts, Vec<uint32_t>& rle_values) {
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
}
void write_rles(std::string& buf, Vec<uint32_t>& rle_counts,
                Vec<uint32_t>& rle_values) {
  assert(rle_counts.size() == rle_values.size());
  write_var_u32(rle_counts.size());
  for (v : rle_counts) {
    write_var_u32(v);
  }
  for (v : rle_values) {
    write_var_u32(v);
  }
}

Status parse_values_idx(int formatId, const rocksdb::Slice& slice, size_t& pos,
                        Vec<uint32_t>& values_idx, uint32_t max_idx) {
  uint32_t n;
  auto status = read_var_u32(slice, pos, n);
  if (!status.ok()) {
    return status;
  }

  values_idx.resize(n);

  size_t pos1 = pos;
  size_t outputlength = 0;

  if (formatId == ObjectSegment::SUBFORMAT_ID_ENUM_FPROF) {
    xc.resize((slice.size() - pos) / 4);
    for (size_t i = 0; i < xc.size(); i++) {
      xc[i] = read_u32_be_unchecked(slice, pos);
    }
    outputlength = n;
    auto in2 = fastpfor128.decodeArray(xc.data(), xc.size(), values_idx.data(),
                                       outputlength);
    if (outputlength > n) {
      return Status::Corruption("Encoded data longer than expected");
    }
    pos = pos1 + 4 * (in2 - xc.data());
  }

  for (auto i = outputlength; i < n; i++) {
    auto s = read_var_u32(slice, pos, values_idx[i]);
    if (!s.ok()) {
      return s;
    }
  }

  for (v : values_idx) {
    // check that the indices are not out of range (with respect to the values
    // that have been previously read from the slice)
    if (v >= max_idx) {
      return Status::Corruption("Enum idx " + std::to_string(v) +
                                " larger than the maximum number of values " +
                                std::to_string(max_idx));
    }
  }

  return Ok();
}


void write_values_idx(std::string& buf, Vec<uint32_t>& values_idx) {
  assert(false && "TODO");
}

}  // namespace yamcs
}  // namespace ROCKSDB_NAMESPACE
