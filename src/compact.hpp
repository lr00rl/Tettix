#pragma once

// Thrift Compact Protocol 游标。Parquet 的 FileMetaData / PageHeader 用这一套。
// 字段表在 parquet.thrift；这里只负责「按类型把字节读掉或跳过」。

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace tettix {

enum CompactType : int {
  T_STOP = 0,
  T_TRUE = 1,
  T_FALSE = 2,
  T_BYTE = 3,
  T_I16 = 4,
  T_I32 = 5,
  T_I64 = 6,
  T_DOUBLE = 7,
  T_BIN = 8,
  T_LIST = 9,
  T_SET = 10,
  T_MAP = 11,
  T_STRUCT = 12,
};

struct Cursor {
  const uint8_t* p = nullptr;
  const uint8_t* end = nullptr;

  explicit Cursor(std::string_view s)
      : p(reinterpret_cast<const uint8_t*>(s.data())),
        end(p + s.size()) {}

  Cursor(const uint8_t* data, size_t n) : p(data), end(data + n) {}

  size_t remaining() const { return static_cast<size_t>(end - p); }

  void need(size_t n) const {
    if (remaining() < n) throw std::runtime_error("thrift: truncated");
  }

  uint8_t u8() {
    need(1);
    return *p++;
  }

  void skip_n(size_t n) {
    need(n);
    p += n;
  }

  // 无符号 LEB128。整数的 zigzag 值、string 长度、list 超长个数都走它。
  uint64_t varint() {
    uint64_t r = 0;
    int s = 0;
    for (;;) {
      uint8_t b = u8();
      r |= uint64_t(b & 0x7f) << s;
      if ((b & 0x80) == 0) return r;
      s += 7;
      if (s > 63) throw std::runtime_error("thrift: varint overflow");
    }
  }

  static int32_t unzig32(uint64_t n) {
    return int32_t((n >> 1) ^ -int32_t(n & 1));
  }
  static int64_t unzig64(uint64_t n) {
    return int64_t((n >> 1) ^ -int64_t(n & 1));
  }

  int32_t i32() { return unzig32(varint()); }
  int64_t i64() { return unzig64(varint()); }
  int16_t i16() { return int16_t(i32()); }

  bool boolean_from_type(int type) const {
    if (type == T_TRUE) return true;
    if (type == T_FALSE) return false;
    throw std::runtime_error("thrift: not a bool field");
  }

  std::string bin() {
    uint64_t n = varint();
    need(n);
    std::string s(reinterpret_cast<const char*>(p), n);
    p += n;
    return s;
  }

  int32_t i32_le() {
    need(4);
    uint32_t v = uint32_t(p[0]) | (uint32_t(p[1]) << 8) | (uint32_t(p[2]) << 16) |
                 (uint32_t(p[3]) << 24);
    p += 4;
    return int32_t(v);
  }

  // 返回 false = STOP。last_id 是当前 struct 里上一个字段号。
  bool next_field(int& last_id, int& field_id, int& type) {
    uint8_t h = u8();
    if (h == 0) return false;
    type = h & 0x0f;
    int delta = (h >> 4) & 0x0f;
    if (delta == 0) {
      field_id = i16();
    } else {
      field_id = last_id + delta;
    }
    last_id = field_id;
    return true;
  }

  struct List {
    int elem_type;
    int32_t size;
  };

  List list() {
    uint8_t h = u8();
    int elem = h & 0x0f;
    int32_t sz = (h >> 4) & 0x0f;
    if (sz == 15) {
      uint64_t n = varint();
      if (n > 0x7fffffff) throw std::runtime_error("thrift: list too long");
      sz = int32_t(n);
    }
    return {elem, sz};
  }

  void skip(int type, bool in_container = false);
  void skip_struct() {
    int last = 0, id = 0, t = 0;
    while (next_field(last, id, t)) skip(t, false);
  }
};

inline void Cursor::skip(int type, bool in_container) {
  // 字段里的 bool 值已经写在 type 里；list/map 里的 bool 是单独一字节。
  if ((type == T_TRUE || type == T_FALSE) && in_container) {
    u8();
    return;
  }
  switch (type) {
    case T_STOP:
      break;
    case T_TRUE:
    case T_FALSE:
      break;
    case T_BYTE:
      u8();
      break;
    case T_I16:
    case T_I32:
    case T_I64:
      varint();
      break;
    case T_DOUBLE:
      skip_n(8);
      break;
    case T_BIN:
      skip_n(varint());
      break;
    case T_LIST:
    case T_SET: {
      auto l = list();
      for (int32_t i = 0; i < l.size; ++i) skip(l.elem_type, true);
      break;
    }
    case T_MAP: {
      uint8_t b = u8();
      if (b == 0) break;
      --p;
      uint64_t n = varint();
      uint8_t kv = u8();
      int kt = (kv >> 4) & 0x0f;
      int vt = kv & 0x0f;
      for (uint64_t i = 0; i < n; ++i) {
        skip(kt, true);
        skip(vt, true);
      }
      break;
    }
    case T_STRUCT:
      skip_struct();
      break;
    default:
      throw std::runtime_error("thrift: unknown type " + std::to_string(type));
  }
}

}  // namespace tettix
