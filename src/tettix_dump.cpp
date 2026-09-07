// tettix dump：对着一份 Parquet 走完 decode。
//
//   1. pread 尾巴 8 字节 → footer 长度
//   2. Compact 解 FileMetaData（schema / offset / min-max）  ← compact.hpp
//   3. 按列 pread chunk → PageHeader → 解压 → levels → PLAIN 值
//
// 只认本仓库 tmp/example.parquet 那一类：Data Page v1、PLAIN、
// UNCOMPRESSED 或 SNAPPY、扁平 OPTIONAL、INT32 / BYTE_ARRAY。

#include "compact.hpp"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace tettix {
namespace {

// parquet.thrift 里的物理类型 / 编码 / 压缩 / 页类型。数字必须对上规范。
enum PhysType : int {
  BOOLEAN = 0,
  INT32 = 1,
  INT64 = 2,
  INT96 = 3,
  FLOAT = 4,
  DOUBLE = 5,
  BYTE_ARRAY = 6,
  FIXED_LEN_BYTE_ARRAY = 7,
};
enum Encoding : int { ENC_PLAIN = 0, ENC_RLE = 3, ENC_RLE_DICT = 8 };
enum Codec : int { UNCOMPRESSED = 0, SNAPPY = 1 };
enum PageType : int { DATA_PAGE = 0, INDEX_PAGE = 1, DICT_PAGE = 2, DATA_PAGE_V2 = 3 };
enum Repetition : int { REQUIRED = 0, OPTIONAL = 1, REPEATED = 2 };

const char* type_name(int t) {
  static const char* n[] = {"BOOLEAN", "INT32", "INT64", "INT96",
                            "FLOAT",   "DOUBLE", "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY"};
  if (t < 0 || t > 7) return "?";
  return n[t];
}
const char* codec_name(int c) {
  switch (c) {
    case UNCOMPRESSED:
      return "UNCOMPRESSED";
    case SNAPPY:
      return "SNAPPY";
    default:
      return "?";
  }
}

struct LocalFile {
  int fd = -1;
  uint64_t sz = 0;
  uint64_t bytes_read = 0;

  explicit LocalFile(const char* path) {
    fd = ::open(path, O_RDONLY);
    if (fd < 0) throw std::runtime_error(std::string("open: ") + std::strerror(errno));
    struct stat st {};
    if (::fstat(fd, &st) < 0) throw std::runtime_error("fstat");
    sz = uint64_t(st.st_size);
  }
  ~LocalFile() {
    if (fd >= 0) ::close(fd);
  }
  LocalFile(const LocalFile&) = delete;
  LocalFile& operator=(const LocalFile&) = delete;
  LocalFile(LocalFile&& o) noexcept : fd(o.fd), sz(o.sz), bytes_read(o.bytes_read) { o.fd = -1; }
  LocalFile& operator=(LocalFile&& o) noexcept {
    if (this != &o) {
      if (fd >= 0) ::close(fd);
      fd = o.fd;
      sz = o.sz;
      bytes_read = o.bytes_read;
      o.fd = -1;
    }
    return *this;
  }

  std::string pread(uint64_t off, uint64_t n) {
    if (off + n > sz) throw std::runtime_error("pread past eof");
    std::string buf(n, '\0');
    uint64_t got = 0;
    while (got < n) {
      ssize_t r = ::pread(fd, buf.data() + got, n - got, off_t(off + got));
      if (r < 0) throw std::runtime_error("pread");
      if (r == 0) throw std::runtime_error("pread eof");
      got += uint64_t(r);
    }
    bytes_read += n;
    return buf;
  }
};

constexpr char kMagic[4] = {'P', 'A', 'R', '1'};

// --- Snappy（page payload 用；不是 Thrift）---

std::vector<uint8_t> snappy_decompress(std::span<const uint8_t> in) {
  Cursor c(in.data(), in.size());
  uint64_t uncompressed = c.varint();  // snappy 自己的 varint，碰巧和 compact 同形
  std::vector<uint8_t> out;
  out.reserve(uncompressed);
  auto copy_from = [&](uint32_t offset, uint32_t len) {
    if (offset == 0 || offset > out.size()) throw std::runtime_error("snappy: bad offset");
    for (uint32_t i = 0; i < len; ++i) out.push_back(out[out.size() - offset]);
  };
  while (c.remaining()) {
    uint8_t tag = c.u8();
    int kind = tag & 3;
    if (kind == 0) {
      uint32_t lit = tag >> 2;
      uint32_t len = 0;
      if (lit < 60) {
        len = lit + 1;
      } else {
        int extra = lit - 59;
        uint32_t n = 0;
        for (int i = 0; i < extra; ++i) n |= uint32_t(c.u8()) << (8 * i);
        len = n + 1;
      }
      c.need(len);
      out.insert(out.end(), c.p, c.p + len);
      c.p += len;
    } else if (kind == 1) {
      uint32_t len = 4 + ((tag >> 2) & 7);
      uint32_t off = (uint32_t(tag & 0xe0) << 3) | c.u8();
      copy_from(off, len);
    } else if (kind == 2) {
      uint32_t len = 1 + (tag >> 2);
      uint32_t off = uint32_t(uint16_t(c.u8()) | (uint16_t(c.u8()) << 8));
      copy_from(off, len);
    } else {
      uint32_t len = 1 + (tag >> 2);
      uint32_t off = uint32_t(c.i32_le());
      copy_from(off, len);
    }
  }
  if (out.size() != uncompressed) throw std::runtime_error("snappy: size mismatch");
  return out;
}

// --- definition / repetition level：RLE + bit-pack hybrid ---

std::vector<uint8_t> decode_levels(Cursor& c, int32_t num_values, int bit_width) {
  if (bit_width == 0) return std::vector<uint8_t>(size_t(num_values), 0);
  int32_t nbytes = c.i32_le();
  if (nbytes < 0) throw std::runtime_error("levels: bad length");
  c.need(size_t(nbytes));
  Cursor s(c.p, size_t(nbytes));
  c.p += nbytes;

  std::vector<uint8_t> out;
  out.reserve(size_t(num_values));
  auto take_value = [&]() -> uint8_t {
    int bytes = (bit_width + 7) / 8;
    uint32_t v = 0;
    for (int i = 0; i < bytes; ++i) v |= uint32_t(s.u8()) << (8 * i);
    return uint8_t(v);
  };

  while (int32_t(out.size()) < num_values && s.remaining()) {
    uint64_t hdr = s.varint();
    if (hdr & 1) {
      int32_t groups = int32_t(hdr >> 1);
      int32_t n = groups * 8;
      int bits = n * bit_width;
      int bytes = (bits + 7) / 8;
      s.need(size_t(bytes));
      for (int32_t i = 0; i < n && int32_t(out.size()) < num_values; ++i) {
        int bitpos = i * bit_width;
        uint32_t v = 0;
        for (int b = 0; b < bit_width; ++b) {
          int idx = (bitpos + b) / 8;
          int off = (bitpos + b) % 8;
          if (s.p[idx] & (1u << off)) v |= 1u << b;
        }
        out.push_back(uint8_t(v));
      }
      s.p += bytes;
    } else {
      int32_t count = int32_t(hdr >> 1);
      uint8_t val = take_value();
      for (int32_t i = 0; i < count && int32_t(out.size()) < num_values; ++i) out.push_back(val);
    }
  }
  if (int32_t(out.size()) != num_values) throw std::runtime_error("levels: short");
  return out;
}

// --- 目录里的对象 ---

struct SchemaElem {
  int type = -1;
  int repetition = -1;
  std::string name;
  int num_children = 0;
  int converted_type = -1;
};

struct Stats {
  bool has_min = false, has_max = false;
  std::string min, max;  // PLAIN，BYTE_ARRAY 不带长度前缀
  int64_t null_count = -1;
};

struct ColumnMeta {
  int type = -1;
  std::vector<int> encodings;
  std::vector<std::string> path;
  int codec = UNCOMPRESSED;
  int64_t num_values = 0;
  int64_t uncompressed = 0;
  int64_t compressed = 0;
  int64_t data_page_offset = 0;
  int64_t dict_page_offset = -1;
  Stats stats;
  int max_def = 0;
  int max_rep = 0;
};

struct RowGroup {
  std::vector<ColumnMeta> columns;
  int64_t num_rows = 0;
};

struct FileMeta {
  int32_t version = 0;
  std::vector<SchemaElem> schema;
  int64_t num_rows = 0;
  std::vector<RowGroup> row_groups;
  std::string created_by;
};

struct PageHeader {
  int type = -1;
  int32_t uncompressed = 0;
  int32_t compressed = 0;
  int32_t num_values = 0;
  int encoding = ENC_PLAIN;
  int def_enc = ENC_RLE;
  int rep_enc = ENC_RLE;
};

template <class Fn>
void each_field(Cursor& c, Fn&& fn) {
  int last = 0, id = 0, type = 0;
  while (c.next_field(last, id, type)) fn(id, type);
}

SchemaElem read_schema_elem(Cursor& c) {
  SchemaElem e;
  each_field(c, [&](int id, int type) {
    switch (id) {
      case 1:
        e.type = (type == T_I32) ? c.i32() : (c.skip(type), e.type);
        break;
      case 3:
        e.repetition = (type == T_I32) ? c.i32() : (c.skip(type), e.repetition);
        break;
      case 4:
        e.name = (type == T_BIN) ? c.bin() : (c.skip(type), e.name);
        break;
      case 5:
        e.num_children = (type == T_I32) ? c.i32() : (c.skip(type), e.num_children);
        break;
      case 6:
        e.converted_type = (type == T_I32) ? c.i32() : (c.skip(type), e.converted_type);
        break;
      default:
        c.skip(type);
        break;
    }
  });
  return e;
}

Stats read_stats(Cursor& c) {
  Stats s;
  each_field(c, [&](int id, int type) {
    switch (id) {
      case 3:
        s.null_count = (type == T_I64) ? c.i64() : (c.skip(type), s.null_count);
        break;
      case 5:
        if (type == T_BIN) {
          s.max = c.bin();
          s.has_max = true;
        } else {
          c.skip(type);
        }
        break;
      case 6:
        if (type == T_BIN) {
          s.min = c.bin();
          s.has_min = true;
        } else {
          c.skip(type);
        }
        break;
      default:
        c.skip(type);
        break;
    }
  });
  return s;
}

ColumnMeta read_column_meta(Cursor& c) {
  ColumnMeta m;
  each_field(c, [&](int id, int type) {
    switch (id) {
      case 1:
        m.type = (type == T_I32) ? c.i32() : (c.skip(type), m.type);
        break;
      case 2:
        if (type == T_LIST) {
          auto l = c.list();
          for (int32_t i = 0; i < l.size; ++i) m.encodings.push_back(c.i32());
        } else {
          c.skip(type);
        }
        break;
      case 3:
        if (type == T_LIST) {
          auto l = c.list();
          for (int32_t i = 0; i < l.size; ++i) m.path.push_back(c.bin());
        } else {
          c.skip(type);
        }
        break;
      case 4:
        m.codec = (type == T_I32) ? c.i32() : (c.skip(type), m.codec);
        break;
      case 5:
        m.num_values = (type == T_I64) ? c.i64() : (c.skip(type), m.num_values);
        break;
      case 6:
        m.uncompressed = (type == T_I64) ? c.i64() : (c.skip(type), m.uncompressed);
        break;
      case 7:
        m.compressed = (type == T_I64) ? c.i64() : (c.skip(type), m.compressed);
        break;
      case 9:
        m.data_page_offset = (type == T_I64) ? c.i64() : (c.skip(type), m.data_page_offset);
        break;
      case 11:
        m.dict_page_offset = (type == T_I64) ? c.i64() : (c.skip(type), m.dict_page_offset);
        break;
      case 12:
        if (type == T_STRUCT) m.stats = read_stats(c);
        else c.skip(type);
        break;
      default:
        c.skip(type);
        break;
    }
  });
  return m;
}

ColumnMeta read_column_chunk(Cursor& c) {
  ColumnMeta m;
  each_field(c, [&](int id, int type) {
    switch (id) {
      case 3:
        if (type == T_STRUCT) m = read_column_meta(c);
        else c.skip(type);
        break;
      default:
        c.skip(type);
        break;
    }
  });
  return m;
}

RowGroup read_row_group(Cursor& c) {
  RowGroup g;
  each_field(c, [&](int id, int type) {
    switch (id) {
      case 1:
        if (type == T_LIST) {
          auto l = c.list();
          for (int32_t i = 0; i < l.size; ++i) g.columns.push_back(read_column_chunk(c));
        } else {
          c.skip(type);
        }
        break;
      case 3:
        g.num_rows = (type == T_I64) ? c.i64() : (c.skip(type), g.num_rows);
        break;
      default:
        c.skip(type);
        break;
    }
  });
  return g;
}

FileMeta read_file_meta(Cursor& c) {
  FileMeta m;
  each_field(c, [&](int id, int type) {
    switch (id) {
      case 1:
        m.version = (type == T_I32) ? c.i32() : (c.skip(type), m.version);
        break;
      case 2:
        if (type == T_LIST) {
          auto l = c.list();
          for (int32_t i = 0; i < l.size; ++i) m.schema.push_back(read_schema_elem(c));
        } else {
          c.skip(type);
        }
        break;
      case 3:
        m.num_rows = (type == T_I64) ? c.i64() : (c.skip(type), m.num_rows);
        break;
      case 4:
        if (type == T_LIST) {
          auto l = c.list();
          for (int32_t i = 0; i < l.size; ++i) m.row_groups.push_back(read_row_group(c));
        } else {
          c.skip(type);
        }
        break;
      case 6:
        m.created_by = (type == T_BIN) ? c.bin() : (c.skip(type), m.created_by);
        break;
      default:
        c.skip(type);
        break;
    }
  });
  return m;
}

PageHeader read_page_header(Cursor& c) {
  PageHeader h;
  each_field(c, [&](int id, int type) {
    switch (id) {
      case 1:
        h.type = (type == T_I32) ? c.i32() : (c.skip(type), h.type);
        break;
      case 2:
        h.uncompressed = (type == T_I32) ? c.i32() : (c.skip(type), h.uncompressed);
        break;
      case 3:
        h.compressed = (type == T_I32) ? c.i32() : (c.skip(type), h.compressed);
        break;
      case 5:
        if (type == T_STRUCT) {
          each_field(c, [&](int fid, int ft) {
            switch (fid) {
              case 1:
                h.num_values = (ft == T_I32) ? c.i32() : (c.skip(ft), h.num_values);
                break;
              case 2:
                h.encoding = (ft == T_I32) ? c.i32() : (c.skip(ft), h.encoding);
                break;
              case 3:
                h.def_enc = (ft == T_I32) ? c.i32() : (c.skip(ft), h.def_enc);
                break;
              case 4:
                h.rep_enc = (ft == T_I32) ? c.i32() : (c.skip(ft), h.rep_enc);
                break;
              default:
                c.skip(ft);
                break;
            }
          });
        } else {
          c.skip(type);
        }
        break;
      default:
        c.skip(type);
        break;
    }
  });
  return h;
}

void attach_levels(FileMeta& m) {
  // schema 是 DFS 拍扁的树。OPTIONAL 让 max_def +1，REPEATED 再让 max_rep +1。
  std::vector<ColumnMeta*> leaves;
  for (auto& g : m.row_groups)
    for (auto& col : g.columns) leaves.push_back(&col);

  size_t leaf_i = 0;
  auto walk = [&](auto&& self, size_t& i, int def, int rep) -> void {
    if (i >= m.schema.size()) throw std::runtime_error("schema: overrun");
    const SchemaElem& e = m.schema[i++];
    int d = def, r = rep;
    if (e.repetition == OPTIONAL) d += 1;
    if (e.repetition == REPEATED) {
      d += 1;
      r += 1;
    }
    if (e.num_children == 0) {
      if (leaf_i >= leaves.size()) throw std::runtime_error("schema: extra leaf");
      leaves[leaf_i]->max_def = d;
      leaves[leaf_i]->max_rep = r;
      leaf_i++;
    } else {
      for (int c = 0; c < e.num_children; ++c) self(self, i, d, r);
    }
  };
  size_t i = 0;
  if (m.schema.empty()) return;
  walk(walk, i, 0, 0);
}

std::string fmt_stat(int type, const std::string& raw) {
  if (type == INT32 && raw.size() == 4) {
    uint32_t v = uint8_t(raw[0]) | (uint32_t(uint8_t(raw[1])) << 8) |
                 (uint32_t(uint8_t(raw[2])) << 16) | (uint32_t(uint8_t(raw[3])) << 24);
    return std::to_string(int32_t(v));
  }
  return raw;
}

struct ColValues {
  std::string name;
  int type = -1;
  std::vector<uint8_t> valid;
  std::vector<int32_t> i32;
  std::vector<std::string> str;
};

ColValues decode_chunk(LocalFile& f, const ColumnMeta& col, int64_t nrows) {
  if (col.dict_page_offset >= 0) throw std::runtime_error("unsupported: dictionary page");
  auto raw = f.pread(uint64_t(col.data_page_offset), uint64_t(col.compressed));
  Cursor c(raw);
  PageHeader ph = read_page_header(c);
  if (ph.type != DATA_PAGE) throw std::runtime_error("unsupported: page type");
  if (ph.encoding != ENC_PLAIN) throw std::runtime_error("unsupported: value encoding");
  if (ph.def_enc != ENC_RLE || ph.rep_enc != ENC_RLE) {
    throw std::runtime_error("unsupported: level encoding");
  }
  c.need(size_t(ph.compressed));
  std::span<const uint8_t> payload(c.p, size_t(ph.compressed));
  std::vector<uint8_t> unpacked;
  if (col.codec == SNAPPY) {
    unpacked = snappy_decompress(payload);
  } else if (col.codec == UNCOMPRESSED) {
    unpacked.assign(payload.begin(), payload.end());
  } else {
    throw std::runtime_error("unsupported: codec");
  }
  if (int32_t(unpacked.size()) != ph.uncompressed) {
    throw std::runtime_error("page uncompressed size mismatch");
  }

  Cursor body(unpacked.data(), unpacked.size());
  // Data Page v1: [rep levels?][def levels?][values]
  if (col.max_rep > 0) {
    (void)decode_levels(body, ph.num_values, col.max_rep);  // 扁平表不会走到
  }
  std::vector<uint8_t> def(size_t(ph.num_values), uint8_t(col.max_def));
  if (col.max_def > 0) def = decode_levels(body, ph.num_values, col.max_def);

  ColValues out;
  out.name = col.path.empty() ? "?" : col.path.back();
  out.type = col.type;
  out.valid.resize(size_t(nrows), 0);
  if (col.type == INT32) out.i32.assign(size_t(nrows), 0);
  if (col.type == BYTE_ARRAY) out.str.assign(size_t(nrows), "");

  int64_t row = 0;
  for (int32_t i = 0; i < ph.num_values; ++i) {
    if (row >= nrows) throw std::runtime_error("more values than rows");
    bool present = def[size_t(i)] == uint8_t(col.max_def);
    out.valid[size_t(row)] = present ? 1 : 0;
    if (present) {
      if (col.type == INT32) {
        out.i32[size_t(row)] = body.i32_le();
      } else if (col.type == BYTE_ARRAY) {
        int32_t n = body.i32_le();
        if (n < 0) throw std::runtime_error("bad BYTE_ARRAY length");
        body.need(size_t(n));
        out.str[size_t(row)].assign(reinterpret_cast<const char*>(body.p), size_t(n));
        body.p += n;
      } else {
        throw std::runtime_error(std::string("unsupported physical type ") + type_name(col.type));
      }
    }
    row++;
  }
  return out;
}

std::string col_name(const ColumnMeta& col) {
  return col.path.empty() ? "?" : col.path.back();
}

struct Reader {
  LocalFile f;
  FileMeta meta;
  uint32_t footer_len = 0;
};

Reader open_reader(const char* path) {
  LocalFile f(path);
  if (f.sz < 8) throw std::runtime_error("file too small");
  auto tail = f.pread(f.sz - 8, 8);
  if (std::memcmp(tail.data() + 4, kMagic, 4) != 0) throw std::runtime_error("bad magic");
  uint32_t footer_len = uint8_t(tail[0]) | (uint32_t(uint8_t(tail[1])) << 8) |
                        (uint32_t(uint8_t(tail[2])) << 16) | (uint32_t(uint8_t(tail[3])) << 24);
  if (uint64_t(footer_len) + 8 > f.sz) throw std::runtime_error("bad footer length");
  auto head = f.pread(0, 4);
  if (std::memcmp(head.data(), kMagic, 4) != 0) throw std::runtime_error("bad head magic");
  auto footer = f.pread(f.sz - 8 - footer_len, footer_len);
  Cursor fc(footer);
  FileMeta meta = read_file_meta(fc);
  attach_levels(meta);
  return Reader{std::move(f), std::move(meta), footer_len};
}

std::vector<const ColumnMeta*> pick_columns(const RowGroup& rg,
                                            const std::vector<std::string>& names) {
  std::vector<const ColumnMeta*> out;
  if (names.empty()) {
    for (const auto& c : rg.columns) out.push_back(&c);
    return out;
  }
  for (const auto& n : names) {
    const ColumnMeta* hit = nullptr;
    for (const auto& c : rg.columns) {
      if (col_name(c) == n) {
        hit = &c;
        break;
      }
    }
    if (!hit) throw std::runtime_error("unknown column: " + n);
    out.push_back(hit);
  }
  return out;
}

void print_table(const std::vector<ColValues>& cols, const std::vector<uint8_t>& keep) {
  for (size_t c = 0; c < cols.size(); ++c) {
    if (c) std::cout << "\t";
    std::cout << cols[c].name;
  }
  std::cout << "\n";
  if (cols.empty()) return;
  int64_t nrows = int64_t(cols[0].valid.size());
  for (int64_t r = 0; r < nrows; ++r) {
    if (!keep.empty() && !keep[size_t(r)]) continue;
    for (size_t c = 0; c < cols.size(); ++c) {
      if (c) std::cout << "\t";
      if (!cols[c].valid[size_t(r)]) {
        std::cout << "NULL";
        continue;
      }
      if (cols[c].type == INT32) std::cout << cols[c].i32[size_t(r)];
      else if (cols[c].type == BYTE_ARRAY) std::cout << cols[c].str[size_t(r)];
    }
    std::cout << "\n";
  }
}

void dump(const char* path) {
  Reader r = open_reader(path);
  std::cout << "file_bytes\t" << r.f.sz << "\n";
  std::cout << "version\t" << r.meta.version << "\n";
  std::cout << "created_by\t" << r.meta.created_by << "\n";
  std::cout << "num_rows\t" << r.meta.num_rows << "\n";
  std::cout << "footer_len\t" << r.footer_len << "\n\n";

  std::cout << "schema\n";
  for (const auto& e : r.meta.schema) {
    std::cout << "  " << e.name;
    if (e.num_children) std::cout << "  group children=" << e.num_children;
    else if (e.type >= 0) std::cout << "  " << type_name(e.type);
    if (e.repetition == OPTIONAL) std::cout << "  OPTIONAL";
    if (e.repetition == REQUIRED) std::cout << "  REQUIRED";
    std::cout << "\n";
  }

  if (r.meta.row_groups.empty()) return;
  const auto& rg = r.meta.row_groups[0];
  std::cout << "\nrow_group 0  rows=" << rg.num_rows << "  cols=" << rg.columns.size() << "\n";
  for (const auto& col : rg.columns) {
    std::cout << "  " << col_name(col) << "  " << type_name(col.type)
              << "  offset=" << col.data_page_offset << "  compressed=" << col.compressed << "  "
              << codec_name(col.codec) << "  def=" << col.max_def;
    if (col.stats.has_min) std::cout << "  min=" << fmt_stat(col.type, col.stats.min);
    if (col.stats.has_max) std::cout << "  max=" << fmt_stat(col.type, col.stats.max);
    std::cout << "\n";
  }

  std::vector<ColValues> cols;
  for (const auto& col : rg.columns) cols.push_back(decode_chunk(r.f, col, rg.num_rows));
  std::cout << "\n";
  print_table(cols, {});
  std::cout << "bytes_read\t" << r.f.bytes_read << "\n";
}

void scan(const char* path, const std::vector<std::string>& names, std::optional<int64_t> gt) {
  Reader r = open_reader(path);
  if (r.meta.row_groups.empty()) throw std::runtime_error("no row groups");
  const auto& rg = r.meta.row_groups[0];
  auto picked = pick_columns(rg, names);
  if (gt && picked.empty()) throw std::runtime_error("--gt needs --col");
  if (gt && picked[0]->type != INT32) throw std::runtime_error("--gt requires INT32 --col");

  std::vector<ColValues> cols;
  cols.reserve(picked.size());
  for (const auto* col : picked) cols.push_back(decode_chunk(r.f, *col, rg.num_rows));

  std::vector<uint8_t> keep;
  if (gt) {
    keep.assign(size_t(rg.num_rows), 0);
    for (int64_t i = 0; i < rg.num_rows; ++i) {
      keep[size_t(i)] = (cols[0].valid[size_t(i)] && int64_t(cols[0].i32[size_t(i)]) > *gt) ? 1 : 0;
    }
  }
  print_table(cols, keep);
  std::cout << "bytes_read\t" << r.f.bytes_read << "\n";
}

}  // namespace
}  // namespace tettix

int main(int argc, char** argv) {
  try {
    if (argc >= 3 && std::string(argv[1]) == "dump") {
      if (argc != 3) {
        std::cerr << "usage: tettix dump FILE.parquet\n";
        return 2;
      }
      tettix::dump(argv[2]);
      return 0;
    }
    if (argc >= 3 && std::string(argv[1]) == "scan") {
      std::vector<std::string> cols;
      std::optional<int64_t> gt;
      for (int i = 3; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--col") {
          if (i + 1 >= argc) throw std::runtime_error("--col needs a name");
          cols.emplace_back(argv[++i]);
        } else if (a == "--gt") {
          if (i + 1 >= argc) throw std::runtime_error("--gt needs a number");
          gt = std::strtoll(argv[++i], nullptr, 10);
        } else {
          throw std::runtime_error("unknown flag: " + a);
        }
      }
      if (gt && cols.empty()) throw std::runtime_error("--gt needs --col");
      tettix::scan(argv[2], cols, gt);
      return 0;
    }
    std::cerr << "usage:\n  tettix dump FILE.parquet\n  tettix scan FILE.parquet [--col NAME ...] [--gt N]\n";
    return 2;
  } catch (const std::exception& e) {
    std::cerr << "error: " << e.what() << "\n";
    return 1;
  }
}
