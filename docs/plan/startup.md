从 **内存里的 Vector** 开始，不要从 SQL、不要从 S3、不要从 JIT。先让 2048 行在 cache 里被 SIMD 过滤跑对；Parquet、计划器和网络都只是往这套向量里灌数据。

整条路线是一条垂直切片不断加厚，不是先铺完每一层再串联。

---

## 总原则

1. **先定执行时的数据布局，再定文件格式和语法。** Vector / selection vector / arena 写错，后面全是返工。
2. **每个能力走同一条环：正确 → 向量化 → 下推/跳过 → 并行 → 再加下一个算子。** 不要一个算子只写标量就去堆功能。
3. **始终有对照物。** 用 DuckDB / pyarrow 当 oracle：同一份 Parquet、同一句 SQL，行集合必须一致（允许列序和浮点误差）。
4. **第一天就开 sanitizer。** Clang + ASan/UBSan；并发上来再加 TSan。引擎里的 bug 90% 是越界、use-after-return、错误的 def level。
5. **热路径禁止 `std::vector` 增长、禁止 `unordered_map`、禁止虚函数。** STL 只用在解析器、计划和测试。

命令行从第一天就存在，哪怕只能：

```text
tettix dump file.parquet
tettix scan file.parquet --col fare --gt 10
```

SQL 是很晚才盖上去的皮。

---

## 阶段 0：实验室，不碰文件

**目标：** 执行模型定死。

要写下的东西：

- bump **arena**（查询级，64 字节对齐，析构时一把扔）
- 物理类型：`I32/I64/F32/F64/BOOL/STRING`（STRING = offset + blob）
- **Vector**：flat / constant 两种 encoding；validity bitmap；**selection vector**
- **DataChunk**：一组 Vector + 行数，默认 `VECTOR_SIZE = 2048`
- 内存表：用 C++ 生成几列假数据（别读 Parquet）
- 一个算子：`Filter(col > const)`，先标量，再 AVX2 写一条 fast path
- 把结果打成表

**过关：** 1e8 行 `i64 > 42` 的吞吐你自己能测；ASan 干净；selection vector 能表示「2048 里留下 17 行」且后面算子只碰这 17 行。

**不要做：** SQL、STL 容器当列存、Arrow RecordBatch、虚函数算子基类上的 per-row `Next()`。

这一阶段的产物是整仓的 ABI。后面 Parquet decoder 的输出必须是这种 Vector，不是「先解码成 `vector<int64_t>` 再转换」。

---

## 阶段 1：InputFile + Parquet 元数据

**目标：** 文件是 `read(offset, len)`，还不是「打开后从头流到尾」。

- `InputFile` 接口：`size()` + `pread` 语义。本地用 `pread`/`io_uring` 都可以，先 `pread`。
- 读尾 8 字节：footer 长度 + `PAR1`。
- 自己写 **Thrift compact**，只解 `FileMetaData`：schema、row group、column chunk、min/max、codec、encoding、offset/size。
- `tettix dump` 打印 schema 和每个 row group 的统计。

**过关：** 能正确读 DuckDB / Spark / pandas 写出来的文件的 footer（不是只读你自己写的）。未知 thrift 字段必须能 skip，否则一遇到新版 LogicalType 就炸。

**不要做：** 解 page；不要为了省事链 Arrow。

对照：用 `parquet-tools` / pyarrow 打出来的 metadata 和你的 dump 对字段。

---

## 阶段 2：最小 decoder（这是第一个真引擎）

只认一种最窄的文件：

- 无压缩、PLAIN、required、INT32/INT64/FLOAT/DOUBLE
- 一个 row group、每列一个 data page

把 column chunk **一次 IO 读进内存**，解析 page header，decode **直接写入阶段 0 的 Vector**。

同时写一个同样窄的 **writer**。没有 writer，你会被 pyarrow 的编码组合牵着走；有 writer，才能做 roundtrip 单测。

**过关：** 自己写的文件能读回来；pyarrow 写的同子集文件也能读；`scan` 输出和 pyarrow 逐行一致。

然后按 **你会在真实文件里碰到的顺序** 加编码，每次只加一种，加完就用 Spark/DuckDB 文件打：

1. optional + definition level（RLE/bitpack）
2. 字典页 + `RLE_DICTIONARY`（字符串先走这里）
3. SNAPPY → ZSTD → LZ4_RAW
4. DataPage V2
5. `DELTA_BINARY_PACKED`（Spark 的 int 列极常见）
6. `BYTE_ARRAY` / UTF8 的 PLAIN

**过关标准不是「支持 Parquet 规范」，是：你工作流里那几类文件（Spark dump、DuckDB export、pandas）能扫对。** 嵌套 LIST/MAP、INT96、DECIMAL 的任意 precision 全部推迟。遇到不支持的 encoding 就明确报错，禁止静默打成 null。

迭代手法：每加一种 encoding，留一个 **golden 文件** 在 `tests/parquet/`，永远回归。

---

## 阶段 3：物理计划 = 算子流水线，SQL 还没有

把阶段 0 的 Filter 和阶段 2 的 Scan 接成 pull 流水线：

`ParquetScan → Filter → Project → Limit → 输出`

Scan 要立刻具备三个能力，否则你在训练自己写慢引擎：

- **列裁剪：** 只读 SELECT/WHERE 用到的 column chunk
- **row group 跳过：** 用 footer 里 min/max 砍整组（这是 S3 上数量级的那一刀）
- **谓词下推：** `fare > 10` 不要先物化所有列再 Filter；能在 Scan 里对单列 SIMD filter，再用 selection vector 去解别的列（late materialization）

**过关：** 宽表（50 列）上 `SELECT two_cols WHERE id > ?` 的 IO 量，接近只读那两三列 chunk 的压缩大小，而不是整文件。

这个阶段结束，你已经有一个「DuckDB 形状」的核：列存、向量、下推、统计裁剪。只是入口还是 flag 而不是 SQL。

---

## 阶段 4：表达式和 HashAgg

表达式先做成 **针对 Vector 的求值器**，不要做成 AST 递归到单元格：

- 字面量、列引用、比较、AND/OR、算术
- `col CMP const` 必须走 SIMD 特化；复杂表达式可以先走选中行上的标量（先正确）
- `IS NULL` / `IS NOT NULL` 碰 validity，不要把 null 当成 0

聚合：

- `count(*) / count(col) / sum / min / max / avg`
- 无 GROUP BY：单行状态
- 有 GROUP BY：开放寻址哈希表，key 内联，字符串 key 进 arena
- **禁止 `std::unordered_map`**

**过关：** 和 DuckDB 比 `GROUP BY vendor` 的结果集（排序后）；再比 1e8 行的 rows/s。哈希表负载因子、是否分区，用 profiler 看，不要先设计成论文。

Join、ORDER BY、窗口函数都还不要做。没有 Agg 的 OLAP 引擎不像引擎；没有 Join 仍然可以当「Parquet 扫描器」用很久。

---

## 阶段 5：SQL 只是绑定层

现在才写解析器。手写递归下降，子集严格：

```sql
SELECT proj [, agg ...]
FROM 'path.parquet'
WHERE expr
GROUP BY ...
ORDER BY ...
LIMIT n
```

路径：Parse → Bind（名字对上 Parquet schema）→ LogicalPlan → 规则改写 → PhysicalPlan。

第一批规则只要三条：

1. 投影下推
2. 过滤下推（含拆 AND）
3. 统计裁剪（逻辑上标记，物理 Scan 执行）

**过关：** 阶段 3/4 能跑的事，SQL 都能跑；`EXPLAIN` 能看出 Filter 在 Scan 里而不是 Scan 之上。

**不要做：** 优化器框架、代价模型、CBO、二十种 join reorder。规则不够用时再加，不要先搭「优化器平台」。

---

## 阶段 6：并行（morsel），仍是单机

先保持 pull 模型能调试，再切：

- 以 **row group**（或把大 row group 切成 morsel）为任务
- 线程池 + 工作窃取
- Scan+Filter 在 worker 里完成
- HashAgg：线程本地表，最后 merge（比一把大锁的全局表快，也比一开始就 lock-free 简单）

**过关：** `nproc` 从 1 拉到 8，扫描型查询接近线性；TSan 干净；结果与单线程一致（无序 SELECT 要比排序后的多重集，不要比行序）。

NUMA、绑核、大页放到你已经能打满 CPU 之后，用 `perf` 证明瓶颈在调度再动。

---

## 阶段 7：S3 —— 同一套 InputFile

本地已经是 `read(offset,len)` 之后，S3 只是另一种实现：

1. 读对象尾 8 字节
2. 拉 footer
3. 按 column chunk 的 offset/size **并行 Range GET**
4. 相邻小 range **合并**，超大 chunk **拆分**
5. 预取：解码当前 chunk 时，下一组已经在飞
6. 客户端用 **AWS CRT（`aws-c-s3`）**，不要长期停留在手写 libcurl+SigV4

先在 MinIO / LocalStack / 假 HTTP range 服务上测正确性，再上同区桶。

**过关：** 同一句 SQL，本地文件和 `s3://...` 结果一致；网络吞吐能打到实例带宽的一个可说得清的比例；footer 那两次往返之外，没有「整对象 GET」。

EC2 上这一阶段才会第一次暴露真实瓶颈：往往是 **range 并发和合并策略**，不是 C++ 慢。用 CRT 的指标和 `perf` 一起看，不要凭感觉加线程。

---

## 阶段 8 以后：按 profiler 加，不按愿望清单加

有真实查询再开下一刀，顺序通常是：

| 你看到的瓶颈 | 再做的事 |
|---|---|
| 表达式解释占 CPU | LLVM JIT 编译表达式，仍保留解释器跑短查询 |
| decode 后立刻 explode 字典 | 字典 encoding 一直保留到 join/agg 不得不展开 |
| page 级仍读了太多 | column/offset index，page 级 skip |
| 多文件目录 | glob、Hive partition 裁剪（partition 列当统计，不读文件） |
| 大 GROUP BY / JOIN 爆内存 | spill 到本地 NVMe |
| 需要多表 | hash join（先 build 小表），再考虑 shuffle（那是分布式，更晚） |
| 短查询延迟 | 减少分配、复用 BufferPool、跳过 JIT |
| 长查询 CPU 打满但 IPC 低 | 检查 VECTOR_SIZE、乱序、hash 冲突、是否标量回退 |

JIT 不要早于「解释型向量化已经正确且可测」。否则你分不清是计划错了还是生成代码错了。

---

## 日常迭代怎么转

每个新内核（一种 encoding、一个算子、一条 SIMD 路径）固定四步：

1. **最小正确实现**（可以标量、可以慢）
2. **和 oracle 比**（DuckDB/pyarrow，随机生成 + golden 文件）
3. **看 `perf` / counters**：是 decode、hash、IO 等待，还是分支误预测
4. **只优化测量到的那一层**（SIMD、下推、合并 range、换 arena）

仓库建议按这个长，而不是按「模块齐全」长：

```text
tettix
  vec/          arena, vector, chunk, sel   ← 阶段 0，几乎冻结
  io/           InputFile, local, s3        ← 接口稳定，实现可换
  parquet/      thrift, page, encodings     ← 阶段 1–2，持续加 encoding
  exec/         scan, filter, project, agg  ← 阶段 3–4、6
  sql/          parse, bind, rewrite        ← 阶段 5
  jit/          很晚才出现
```

`vec/` 的 ABI 尽量冻：谁都可以往里填，谁都不能随便改 layout。这是 DuckDB 能迭代十年的原因之一。

---

## 明确的「还没到」

不到阶段 6 打满单机，不要想：分布式、Postgres 协议、目录服务、自己的文件格式、GPU、代价模型框架。

不到阶段 2 的 decoder 对几种真实文件稳定，不要写优化器——你优化的是错数据。

不到 Scan 能列裁剪 + 统计跳过，不要接 S3——你会用网络放大一个本来就错的 IO 模式。

---

## 从哪里开始（第一刀）

今天就可以写的最小闭环：

1. `Arena` + `Vector` + `DataChunk`
2. 内存里生成 1 列 `i64`
3. AVX2 的 `>` filter + selection vector
4. 打印留下多少行、花了多少 ns/row

这一下午如果 layout 是对的，后面所有阶段都是往 DataChunk 上接管子。layout 不对，Parquet 写得再全也是库存积压。

下一步才是 `PAR1` 和 footer，不是 `SELECT`。
