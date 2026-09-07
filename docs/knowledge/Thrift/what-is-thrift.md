# Thrift（对 Parquet 而言）

**解 footer 之前不用读这篇。** 主线在 [what-is-parquet.md](../parquet/what-is-parquet.md)。

Parquet 不跑 Thrift RPC。它只是用 Compact 协议，按 `parquet.thrift` 的字段表，把「目录」和「页头」打成字节。

要写的是一个能 **跳过不认识字段** 的解包器，不是 Thrift 框架。

- Compact 规格：<https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md>
- 字段表：<https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift>

文件里只有两处是这套编码：尾巴的 `FileMetaData`，每个 page 前的 `PageHeader`。值、level、Snappy 都不是。

---

## 作弊条

一个字节经常同时表示「这是第几个字段」和「什么类型」。`00` = 当前结构结束。

| 低 4 位 | 类型 | 后面 |
|---------|------|------|
| 0 | STOP | 无 |
| 1 / 2 | bool 真 / 假 | 无 |
| 5 / 6 | i32 / i64 | zigzag varint |
| 8 | 字符串 | varint 长度 + 原文 |
| 9 | list | 见下 |
| 12 | 嵌套结构 | 字段序列，直到 STOP |

- 字段：`delta` 为 1..15 时，一字节 `(delta << 4) | type`；否则 type 一字节 + zigzag i16 的绝对 id。
- 整数 zigzag：`n = (u >> 1) ^ -(u & 1)`。字节 `02` → 1，`00` → 0。
- list 个数 ≤ 14：`(count << 4) | elem_type`。`5c` = 5 个 struct。
- 不认识的字段：按类型把字节读掉。STRUCT 递归到 STOP。新版 Parquet 会加字段，跳不过去就会炸。

对着样本的逐字节标注在 [example-walkthrough.md](../parquet/example-walkthrough.md)，解 page / footer 卡住时再翻。
