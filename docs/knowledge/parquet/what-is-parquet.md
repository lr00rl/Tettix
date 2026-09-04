## Docs
- https://parquet.apache.org/docs/




### Create An example.parquet

```shell
>$ duckdb
DuckDB v1.5.5 (Variegata)
Enter ".help" for usage hints.
memory D COPY (
             SELECT *
             FROM (VALUES
                 (1, 'Alice', 25, 'Beijing'),
                 (2, 'Bob', 30, 'Shanghai'),
                 (3, 'Charlie', 28, 'Shenzhen')
             ) AS t(id, name, age, city)
         ) TO 'example.parquet' (FORMAT PARQUET);
```

> Parquet file always starts and ends with `PAR1` bytes.

Mostly a parquet file structured as:

```txt
┌──────────────────────────────┐
│ Header                       │
│ "PAR1"                       │
├──────────────────────────────┤
│                              │
│ Row Group 0                  │
│   ├── Column Chunk           │
│   │    └── Page              │
│   ├── Column Chunk           │
│   │    └── Page              │
│   └── ...                    │
│                              │
│ Row Group 1                  │
│   └── ...                    │
│                              │
├──────────────────────────────┤
│ File Metadata                │
│   Thrift encoded             │
├──────────────────────────────┤
│ metadata length: 4 bytes     │
├──────────────────────────────┤
│ "PAR1"                       │
└──────────────────────────────┘
```


#### View Source Bytes

```shell
>$ xxd example.parquet
00000000: 5041 5231 1500 1524 1528 2c15 0615 0015  PAR1...$.(,.....
00000010: 0615 0600 0012 4402 0000 0006 0101 0000  ......D.........
00000020: 0002 0000 0003 0000 0015 0015 4215 462c  ............B.F,
00000030: 1506 1500 1506 1506 0000 2180 0200 0000  ..........!.....
00000040: 0601 0500 0000 416c 6963 6503 0000 0042  ......Alice....B
00000050: 6f62 0700 0000 4368 6172 6c69 6515 0015  ob....Charlie...
00000060: 2415 282c 1506 1500 1506 1506 0000 1244  $.(,...........D
00000070: 0200 0000 0601 1900 0000 1e00 0000 1c00  ................
00000080: 0000 1500 1552 1556 2c15 0615 0015 0615  .....R.V,.......
00000090: 0600 0029 a002 0000 0006 0107 0000 0042  ...)...........B
000000a0: 6569 6a69 6e67 0800 0000 5368 616e 6768  eijing....Shangh
000000b0: 6169 0800 0000 5368 656e 7a68 656e 1502  ai....Shenzhen..
000000c0: 195c 3500 180d 6475 636b 6462 5f73 6368  .\5...duckdb_sch
000000d0: 656d 6115 0800 1502 2502 1802 6964 2522  ema.....%...id%"
000000e0: 0015 0c25 0218 046e 616d 6525 0000 1502  ...%...name%....
000000f0: 2502 1803 6167 6525 2200 150c 2502 1804  %...age%"...%...
00000100: 6369 7479 2500 0016 0619 1c19 4c26 001c  city%.......L&..
00000110: 1502 1915 0019 1802 6964 1502 1606 1646  ........id.....F
00000120: 164a 2608 3c18 0403 0000 0018 0401 0000  .J&.<...........
00000130: 0016 0028 0403 0000 0018 0401 0000 0011  ...(............
00000140: 1100 0000 2600 1c15 0c19 1500 1918 046e  ....&..........n
00000150: 616d 6515 0216 0616 6416 6826 523c 1807  ame.....d.h&R<..
00000160: 4368 6172 6c69 6518 0541 6c69 6365 1600  Charlie..Alice..
00000170: 2807 4368 6172 6c69 6518 0541 6c69 6365  (.Charlie..Alice
00000180: 1111 0000 0026 001c 1502 1915 0019 1803  .....&..........
00000190: 6167 6515 0216 0616 4616 4a26 ba01 3c18  age.....F.J&..<.
000001a0: 041e 0000 0018 0419 0000 0016 0028 041e  .............(..
000001b0: 0000 0018 0419 0000 0011 1100 0000 2600  ..............&.
000001c0: 1c15 0c19 1500 1918 0463 6974 7915 0216  .........city...
000001d0: 0616 7416 7826 8402 3c18 0853 6865 6e7a  ..t.x&..<..Shenz
000001e0: 6865 6e18 0742 6569 6a69 6e67 1600 2808  hen..Beijing..(.
000001f0: 5368 656e 7a68 656e 1807 4265 696a 696e  Shenzhen..Beijin
00000200: 6711 1100 0000 16e4 0216 0626 0816 f402  g..........&....
00000210: 0028 2844 7563 6b44 4220 7665 7273 696f  .((DuckDB versio
00000220: 6e20 7631 2e35 2e35 2028 6275 696c 6420  n v1.5.5 (build
00000230: 6438 6364 6161 3333 6664 2919 4c1c 0000  d8cdaa33fd).L...
00000240: 1c00 001c 0000 1c00 0000 8c01 0000 5041  ..............PA
00000250: 5231                                     R1
```

Let's look into it:

```shell
offset
0x0000
  │
  ├── PAR1                         ← Parquet magic
  │
  ├── Row Group
  │    ├── Column Chunk: id
  │    ├── Column Chunk: name
  │    ├── Column Chunk: age
  │    └── Column Chunk: city
  │
  ├── File Metadata                ← Thrift 编码
  │
  ├── 4 bytes metadata length
  │
  └── PAR1                         ← Parquet magic
0x0250
```

Organized as Row Group:

```shell
Row Group 0
│
├── id
│
├── name
│
├── age
│
└── city
```

```shell
0x04 ──────────────┐
                   │
                   ↓
              Column Chunk: id
                   │
                   ├── Page Header
                   └── Page Data

0x5f ──────────────┐
                   ↓
              Column Chunk: name
                   │
                   ├── Page Header
                   └── Page Data

...

0xc0 ──────────────→ FileMetaData
```


---
#### 回到你的 Parquet 文件

你这个文件最后：

```text
00000240:
1c00 001c 0000 1c00 0000 8c01 0000 5041

00000250:
5231
```

可以拆成：

```text
...
8c 01 00 00    ← 4 bytes
50 41 52 31    ← 4 bytes
```

按照 Parquet footer 的规定：

```text
8c 01 00 00
│
└── uint32 little-endian
    = 396
```

所以：

```text
文件大小 = 596

596
- 396        metadata
- 4          metadata length
- 4          PAR1
────────
192
```

因此：

```text
offset 0x00  ───────────────┐
                            │
             data           │ 192 bytes
                            │
offset 0xc0  ───────────────┘
             FileMetaData   │ 396 bytes
                            │
offset 0x24c                │
             8c 01 00 00    │ 4 bytes
             = 396          │
                            │
offset 0x250
             PAR1           │ 4 bytes
                            ↓
```

这就是为什么我前面能确定：

```text
metadata starts at 0xc0
metadata length = 396
```



这两段的解析方法完全不同：

```text
0x00 ───────────── 0xbf    Data / Row Group
0xc0 ───────────── 0x24b   FileMetaData
0x24c ──────────── 0x24f   metadata_length
0x250 ──────────── 0x253   PAR1
```

其中最关键的一点是：

**前 192 bytes 不是一个整体结构，而是多个 Page Header + Page Data + Column Chunk 拼起来的；后 396 bytes 则是一个 Thrift Compact Protocol 编码的 `FileMetaData` 对象。**

所以我们分别看。

---

## 一、`0x00 ~ 0xbf`：Data 部分怎么解析？

你现在看到：

```text
00000000: 5041 5231 1500 1524 1528 2c15 0615 0015
00000010: 0615 0600 0012 4402 0000 0006 0101 0000
00000020: 0002 0000 0003 0000 0015 0015 4215 462c
00000030: 1506 1500 1506 1506 0000 2180 0200 0000
00000040: 0601 0500 0000 416c 6963 6503 0000 0042
00000050: 6f62 0700 0000 4368 6172 6c69 6515 0015
00000060: 2415 2815 2c15 0615 1500 1506 1506 0000
00000070: 1244 0200 0000 0601 1900 0000 1e00 0000
00000080: 1c00 0000 1500 1552 1556 2c15 0615 0015
00000090: 0615 0600 0029 a002 0000 0006 0107 0000
000000a0: 0042 6569 6a69 6e67 0800 0000 5368 616e  ...
```

你不能简单地：

```text
前 192 bytes = 数据
```

然后逐字节解释。

正确的方法是：

```text
PAR1
  ↓
Row Group
  ↓
Column Chunk
  ↓
Page
  ↓
Page Header
  ↓
Page Data
```

---

# 二、先理解 Column Chunk

你的表：

```text
id       name       age       city
1        Alice      25        Beijing
2        Bob        30        Shanghai
3        Charlie    28        Shenzhen
```

Parquet **不是按照行存储**：

```text
1 Alice 25 Beijing
2 Bob   30 Shanghai
3 Charlie 28 Shenzhen
```

而是列式：

```text
id:
1
2
3

name:
Alice
Bob
Charlie

age:
25
30
28

city:
Beijing
Shanghai
Shenzhen
```

因此你的 Row Group 里面实际上有：

```text
Row Group 0
│
├── Column Chunk: id
│
├── Column Chunk: name
│
├── Column Chunk: age
│
└── Column Chunk: city
```

这就是为什么你在 hex 中会看到：

```text
Alice
Bob
Charlie
```

然后后面又看到：

```text
Beijing
Shanghai
Shenzhen
```

它们不是按照原始表格的行顺序出现的。

---

# 三、最重要的一点：`Page Header` 也是二进制结构

比如这里：

```text
15 00 15 24 15 28 2c 15 06 ...
```

你可能会问：

> `15` 是不是某个数字？

不能这么看。

它实际上是 **Thrift Compact Protocol** 的编码。

概念上，Parquet 的 Page Header 是：

```text
PageHeader {
    type
    uncompressed_page_size
    compressed_page_size
    ...
}
```

所以你看到：

```text
15 00 15 24 ...
```

实际上是：

```text
Thrift bytes
    ↓
PageHeader
    ↓
{
    type = DATA_PAGE
    uncompressed_page_size = ...
    compressed_page_size = ...
}
```

因此研究 Parquet binary format 时，必须同时学：

```text
Parquet format
+
Thrift Compact Protocol
```

---

# 四、你的 `Alice` 其实已经非常容易定位

例如：

```text
00000040:
06 01 05 00 00 00 41 6c 69 63 65
```

这里：

```text
41 6c 69 63 65
```

ASCII：

```text
Alice
```

后面：

```text
42 6f 62
```

是：

```text
Bob
```

再后面：

```text
43 68 61 72 6c 69 65
```

是：

```text
Charlie
```

因此你已经可以确认：

```text
name Column Chunk
        ↓
    Page Data
        ↓
Alice
Bob
Charlie
```

但是具体的 `06 01 05 00 00 00` 到底分别是什么，不能仅靠肉眼，需要结合 `PageHeader` 的 encoding 来解释。

---

# 五、那 `0xc0 ~ 0x24b` 怎么解析？

这一段就简单很多：

```text
0xc0
 ↓
FileMetaData
 ↓
0x24b
```

它是一个完整的：

```text
parquet.thrift
    FileMetaData
```

通过：

```text
Thrift Compact Protocol
```

序列化之后的结果。

概念上的对象类似：

```text
FileMetaData {
    version
    schema
    num_rows
    row_groups
    key_value_metadata
    created_by
    column_orders
}
```

你的 hex：

```text
000000c0:
195c 3500 180d 6475 636b 6462 5f73 6368
656d 6115 0800 1502 2502 1802 6964 2522
0015 0c25 0218 046e 616d 6525 0000 1502
2502 1803 6167 6525 2200 150c 2502 1804
6369 7479 ...
```

里面已经可以看到：

```text
duckdb_schema
id
name
age
city
```

这是因为 Thrift 编码里面有字符串字段，而字符串本身是原始 bytes，所以 ASCII 文本直接暴露出来了。

---

# 六、这里最值得学的是 Thrift Compact Protocol

例如你看到：

```text
15 08 00 15 02 25 02 18 02 69 64
```

不要把它当成：

```text
15
08
00
15
02
25
...
```

一个一个独立整数。

它实际上类似：

```text
┌──────────────────────────────┐
│ Thrift Compact Protocol      │
│                              │
│ field header                 │
│ field type                   │
│ field id                     │
│ value                        │
│ field header                 │
│ ...                          │
└──────────────────────────────┘
```

也就是说：

```text
15
```

可能同时包含：

```text
field type
+
field id delta
```

这就是 Compact Protocol 的核心思想之一。

---

# 七、怎么真正把它拆出来？

最好的办法不是手工猜，而是让 DuckDB / PyArrow 告诉你**结构信息**，然后拿 offset 回到 hex。

例如 DuckDB：

```sql
SELECT *
FROM parquet_metadata('example.parquet');
```

还可以：

```sql
SELECT *
FROM parquet_file_metadata('example.parquet');
```

以及：

```sql
SELECT *
FROM parquet_schema('example.parquet');
```

这样你可以得到：

```text
Row Group
Column Chunk
  ↓
file_offset
data_page_offset
total_compressed_size
total_uncompressed_size
num_values
encoding
```

然后再：

```bash
xxd -s <offset> -l <size> example.parquet
```

这样就不是：

> “我看到了 `15 24 15 28`，这是什么？”

而是：

> “DuckDB 告诉我这个 Page 从 `0x04` 开始，我现在去看 `0x04`。”

这会容易很多。

---

# 八、你这个文件尤其适合做一个实验

建议直接执行：

```sql
SELECT
    row_group_id,
    column_id,
    path_in_schema,
    file_offset,
    data_page_offset,
    total_compressed_size,
    total_uncompressed_size,
    num_values,
    encodings
FROM parquet_metadata('example.parquet');
```

然后你会得到类似：

```text
row_group | column | path   | offset | size | encoding
-------------------------------------------------------
0         | 0      | id     | ...    | ...  | ...
0         | 1      | name   | ...    | ...  | ...
0         | 2      | age    | ...    | ...  | ...
0         | 3      | city   | ...    | ...  | ...
```

然后拿其中一个，比如 `name`：

```text
name
 ↓
Column Chunk
 ↓
Page
 ↓
Page Header
 ↓
Page Data
 ↓
Alice Bob Charlie
```

最后再用：

```bash
xxd -s <data_page_offset> -l <size> example.parquet
```

对照起来看。

---

## 九、最终你应该建立这样一张“地图”

你的这个 596-byte 文件，可以理解成：

```text
                 example.parquet
                       │
          ┌────────────┴────────────┐
          │                         │
       Data                        Footer
       192 B                       396 B
          │                         │
       Row Group                FileMetaData
          │                         │
    ┌─────┼─────┬─────┐       Thrift Compact
    ↓     ↓     ↓     ↓       Protocol
   id    name   age   city          │
    │     │     │     │             ├── schema
    │     │     │     │             ├── num_rows = 3
    │     │     │     │             ├── row_groups
    │     │     │     │             ├── column metadata
    │     │     │     │             └── created_by
    │     │     │     │
   Page  Page  Page  Page
    │     │     │     │
    ↓     ↓     ↓     ↓
  1,2,3 Alice... 25... Beijing...
```

而最后：

```text
metadata length = 396
PAR1
```

负责告诉 reader：

> “从文件末尾往前数 396 bytes，就是 FileMetaData。”

所以 Parquet reader 的读取过程实际上非常漂亮：

```text
打开文件
  ↓
seek(-8)
  ↓
读取 metadata_length
  ↓
读取最后的 PAR1
  ↓
seek(file_size - 8 - metadata_length)
  ↓
读取 FileMetaData
  ↓
得到 RowGroup / ColumnChunk / Page 的位置
  ↓
精准 seek 到需要的 Column Chunk
  ↓
读取 Page
```

这就是为什么 Parquet 能够**不读取整个文件，就直接读取某几列/某几个 Row Group**。

如果你想继续“手拆”这个文件，下一步最值得做的是**只拆 `0xc0 ~ 0x24b` 的 FileMetaData**：我可以直接拿你这 396 bytes，逐 byte 给你标出 `Thrift Compact Protocol → FileMetaData → SchemaElement → RowGroup → ColumnChunk` 是怎么对应起来的。

