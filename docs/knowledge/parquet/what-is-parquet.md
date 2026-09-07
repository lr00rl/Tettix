# Parquet

一张表，按**列**存，尾巴上挂一份**目录**。

今天只需要这一张图：

```
PAR1 | 各列的数据块 | 目录 | 目录有多长 | PAR1
```

读文件 = 先看尾巴那 8 字节，找到目录，再按目录去抽某一列。不要从头扫，也不要先解 `Alice`。

仓库里的样本：`tests/parquet/example.parquet`（3 行：id / name / age / city）。

```
make test          # dump + 按列 scan 的回归
make dump          # 打印目录和整表
./build/tettix scan tests/parquet/example.parquet --col id --gt 1
```

字节级拆解（哪天卡住再打开）：[example-walkthrough.md](example-walkthrough.md)  
打包目录用的编码（解 footer 时再看）：[Thrift 笔记](../Thrift/what-is-thrift.md)

## 规范（备用）

- <https://parquet.apache.org/docs/>
- <https://github.com/apache/parquet-format>
- 字段表：<https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift>

---

## 一次只做一步

| 步 | 你在干什么 | 明确不干什么 |
|----|------------|--------------|
| 1 | 读最后 8 字节：长度 + `PAR1` | 不解数据、不碰 Thrift |
| 2 | 把目录那一坨读出来，能打印 schema / 每列 offset / min-max | 不解 page |
| 3 | 按 offset 只读一列，解出 `1, 2, 3` | 不管压缩、字典、嵌套 |

`docs/plan/startup.md` 的阶段 1 就是第 1–2 步。第 3 步是阶段 2。

对照用 DuckDB，不要用眼睛猜 hex：

```sql
SELECT * FROM parquet_schema('tmp/example.parquet');
SELECT path_in_schema, data_page_offset, stats_min_value, stats_max_value
FROM parquet_metadata('tmp/example.parquet');
```

---

## 目录里有什么（先当 JSON 看）

```
FileMetaData
  schema:     有哪些列、什么类型
  num_rows:   3
  row_groups: 每一列存在文件的哪一段、编码、min/max
```

有了这个，就可以：只读用到的列；`id` 最大值是 3 时，`WHERE id > 100` 整组跳过。

这就是引擎在乎 Parquet 的原因。值怎么压缩，以后再说。
