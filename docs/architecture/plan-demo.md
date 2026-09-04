结论先说：**要极限速度，主语言用 C++20/23，编译器用 Clang，执行层自己写向量化 + morsel 并行，热路径再上 LLVM JIT。S3 用 AWS CRT（`aws-c-s3`），本地盘用 `io_uring`。不要用 JVM / Go / Python 当执行引擎，也不要把 Arrow 当内部运行时格式。**

Rust 能写到很快（DataFusion / Polars），但你要的是「不考虑开发成本的天花板」。这个领域现在跑在最前面的引擎几乎全是 C++：DuckDB、ClickHouse、Velox、Photon、Umbra。差距不在语法，而在：自定义 arena、buffer 复用、SIMD、别名控制、和 LLVM 粘在一起时，C++ 没有借还规则卡你。

---

## 语言和「框架」分别是什么

「框架」在这里不该是 ORM 或某个 SQL 库。热路径上你只能依赖几块已经被打到极限的 C 库，其余全部自己写。

| 层 | 用什么 | 不要用什么 |
|---|---|---|
| 语言 | C++20/23 | Java/Scala、Go、Python、Node |
| 编译器 | Clang + LTO；热核可手写 intrinsic / ISPC | 只开 `-O2` 的发行构建 |
| 查询编译 | LLVM ORC JIT（第二阶段） | 永远解释 AST |
| 内存 | 查询级 arena + mimalloc；64B 对齐列存 | `new`/`malloc` 散落在算子里、STL 分配器走热路径 |
| 本地 IO | `io_uring`，注册缓冲区、批量提交 | 每列 `pread` + 线程池死等 |
| S3 | **AWS CRT `aws-c-s3`**（并行 Range GET、HTTP/2、校验、重试） | 旧版 AWS SDK 默认 HTTP、先整对象下到盘、FUSE/Mountpoint 当执行层 |
| 压缩 | zstd / lz4 / snappy 的 C 库 | 自己重写压缩器（收益极低） |
| 交换格式 | 对外可以 Arrow | **对内不要 Arrow RecordBatch** |

Arrow / parquet-cpp 适合当参考实现和测试对照，不适合当你的执行内存布局。DuckDB、Velox 都有自己的 Vector：Flat / Constant / Dictionary / Sequence + selection vector。字典先保持字典，能不 explode 就不 explode。

---

## 执行模型：这才是数量级差距

解析器、SQL 方言几乎不影响速度。快慢在流水线：

1. **向量化执行**（MonetDB / VectorWise / DuckDB）
   一次处理 2048～4096 行，不是 volcano 逐 tuple。

2. **Morsel-driven 并行**（HyPer / DuckDB）
   把数据切成 morsel，工作窃取，而不是「一个 pipeline 绑死一个线程」或 MapReduce。

3. **谓词下推 + late materialization**
   先用 row group / page 的 min-max、bloom、column index 砍数据；filter 只碰需要的列；投影列最后再 decode。

4. **再往上的天花板：把 pipeline 编成机器码**
   解释型向量化是 DuckDB 这条路；ClickHouse / Umbra / Photon 会把表达式甚至整段 pipeline JIT 成带 SIMD 的 native。你要极限，架构上要留这个口：第一阶段向量化解释器，第二阶段 LLVM codegen。不要一上来就 JIT，但数据结构要从第一天按「可编译」来。

算子顺序建议：`Scan → Filter → Project → HashAgg/Join → Sort/Limit`。Hash join / hash agg 用开放寻址 Swiss-table 风格，分区、线程本地表再 merge。不要用 `std::unordered_map`。

---

## Parquet：自己写 decoder，但目标不是「能读」，是「对着执行向量 decode」

自己写是对的，范围要狠：

- 先打穿：Thrift compact footer、DataPage V1/V2、PLAIN、RLE/bitpack、字典页、`DELTA_BINARY_PACKED`（Spark 文件特别多）。
- 压缩：UNCOMPRESSED / SNAPPY / ZSTD / LZ4_RAW。
- **一次 IO 读整个 column chunk**（或按 page 对齐的 coalesced range），在内存里切 page，不要每页一次网络往返。
- decode 直接进你的 Vector；字典列保持 dict encoding。
- 用 footer 统计跳 row group；有 column/offset index 就跳 page。
- 嵌套/可选列用 definition / repetition level，但 v1 可以先只做 primitive + optional。

对照实现只读源码，不要链进进程：DuckDB `parquet_reader`、Velox Parquet、parquet-cpp。它们的布局和缓冲策略比 Thrift 规范更值得抄。

---

## S3：网络才是 EC2 上真正的墙

在 EC2 上扫 Parquet，CPU 经常不是瓶颈，**Range 请求的并发、合并、和是否同区**才是。

做法：

- 对象当文件：`HEAD`/读尾 8 字节拿 footer 长 → 拉 footer → 按 column chunk 的 offset/size 发 **并行 Range GET**。
- Range 要合并（相邻小 chunk 合成大请求），也要拆（单 chunk 太大时分片）。目标是打满实例网卡（50/100/200 Gbps）。
- 客户端必须是 **AWS CRT**，不是「自己用 libcurl 拼 SigV4」当长期方案。CRT 这条是 AWS 自己打 S3 吞吐的路径。
- 凭证：环境变量 / instance profile（IMDSv2）/ IRSA。同区、S3 Gateway VPC Endpoint。延迟极敏感再看 S3 Express One Zone。
- 解压、decode、filter 和网络重叠：prefetch 下一组 chunk，不要等整文件。
- 不要：整对象 GET、写临时文件再 mmap、通过 Mountpoint FUSE 当执行引擎的 IO 层。

本地盘（实例 NVMe）用 `io_uring` + 大页；和 S3 共用同一套 `InputFile(offset, len)` 接口，扫描器不关心数据在哪。

---

## EC2 硬件：语言定错，机型也能把上限锁死

按负载选，而不是「随便一台 c6i」：

- **纯算/JIT/SIMD**：Intel `c7i`/`m7i`（AVX-512）或 AMD `c7a`/`c8a`。
- **要自己养 ARM 内核**：Graviton `c8g`/`r8g`（吞吐/美元往往最好，但必须有 NEON/SVE 路径，不能只写 AVX）。
- **本地 Parquet 湖**：`i4i`/`i7i` 这类实例盘，避免 EBS 当热扫描路径。
- **S3 扫描**：看 **网络带宽和 PPS**，不要只看 vCPU。同 AZ、同 region。
- 系统：新内核（你这边已经是 6.x，`io_uring` 可用）、关透明巨页的随机坑或按实测调、NUMA binding、热线程 pin 核。极限调优才上 `isolcpus`。

GPU（cuDF / 自写 CUDA）只有「计算密度极高、数据已经在 GPU 或值得付 PCIe 拷贝」时才有意义。DuckDB 这类 S3 扫描 + filter/agg，多数时候是网络和 decode，不是 GPU 的主场。第一版不要碰。

---

## 建议你动手的顺序（仍然是方向，不是实现）

1. **内存向量 + SIMD filter/project**（本地随机列，先把 morsel 和 selection vector 做对）。
2. **Parquet：footer + 统计跳过 + PLAIN/字典 decode 进向量**。
3. **物理计划：Scan/Filter/Project/Agg/Limit**，谓词下推到 Scan。
4. **同一套 InputFile 接上 CRT Range GET**。
5. **表达式 LLVM JIT**，替换解释型向量化热核。
6. 再做 join、spill、多文件目录、schema evolution。

解析器可以很简陋（手写递归下降就够）。优化器先做规则：投影下推、过滤下推、统计裁剪。代价模型可以很晚再做。

---

## 明确不要走的路

- 用 DataFusion / DuckDB / ClickHouse 当「自己的引擎」再包一层——那是产品，不是你要写的东西。
- 用 Arrow 当执行层：交换可以，运行时会把最后 10%～30% 锁死（多余 copy、没有 selection vector 语义、字典过早展开）。
- 用 Spark 模型学执行：task 太重，延迟和 CPU cache 都不对。
- 先写分布式。单机把一条流水线打满一块 `c7i.8xlarge` / 一张 100Gbps 网卡，再谈 shuffle。DuckDB 证明单机向量化可以先走很远。

**一句话：C++23 + Clang + 自有列式 Vector + morsel 流水线 + 自写 Parquet decoder + `aws-c-s3` + `io_uring`，架构预留 LLVM JIT。这是目前在 EC2 上写这类引擎的天花板组合。**
