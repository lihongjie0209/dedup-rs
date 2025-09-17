# dedup-rs

一个用 Rust 编写的高性能重复文件扫描工具，面向大规模目录与海量文件的去重场景。

核心思路：多阶段过滤流水线最大化吞吐、最小化不必要的 I/O。

- 阶段 1：按文件大小分组（并行遍历）
- 阶段 2：计算“部分哈希”（文件头 4KB + 尾 4KB，BLAKE3）筛掉大多数非重复项
- 阶段 3：对候选组计算完整 BLAKE3 哈希（64KB 分块流式读取）最终确认

并行能力覆盖目录遍历与哈希计算，尽量饱和多核 CPU。

## 特性

- 并行目录扫描：`jwalk` + `rayon::par_bridge` 提升遍历速度
- 快速哈希：使用 `BLAKE3`（更快且安全）
- 部分哈希策略：头尾各 4KB 组合，有效减少全量哈希次数
- 多种输出格式：txt/csv/json，并可写入文件
- 统计与指标：总文件/总字节数、候选组数、重复组/文件数、可回收空间、各阶段耗时、哈希字节量等

## 构建

需要安装 Rust（stable）。Windows 用户推荐通过 https://rustup.rs/ 安装。

```powershell
cargo build --release
```

构建完成后可执行文件位于：`target/release/dedup-rs(.exe)`。

## 使用方法

最简用法（扫描某个目录）：

```powershell
# 使用发布版二进制运行
./target/release/dedup-rs "C:\\path\\to\\scan"

# 或通过 cargo 运行（注意 -- 与参数分隔）
cargo run --release -- "C:\\path\\to\\scan"
```

### 指定输出格式与写入文件

默认以文本（txt）格式输出到标准输出。支持 `--format` 选择 `txt | csv | json`，`-o/--output` 写入文件：

```powershell
# 文本（默认）输出到控制台
./target/release/dedup-rs "C:\\path\\to\\scan"

# CSV 输出到控制台
./target/release/dedup-rs "C:\\path\\to\\scan" --format csv

# JSON 输出到控制台
./target/release/dedup-rs "C:\\path\\to\\scan" --format json

# 将 JSON 写入文件
./target/release/dedup-rs "C:\\path\\to\\scan" --format json -o .\\duplicates.json
```

文本/CSV 会打印重复文件分组；JSON 会输出一个对象，包含 `metrics`（指标）与 `groups`（重复文件组）。

### 指标说明（metrics）

- total_files：扫描到的文件数量
- total_bytes：扫描到的文件总字节数
- candidate_groups：按大小分组后候选组数量
- partial_groups：部分哈希后剩余组数量
- duplicate_groups：最终确认的重复文件组数量
- duplicate_files：所有重复组中的文件数量总和
- reclaimable_bytes：若每组保留 1 份，其余删除可回收的字节数估算
- bytes_hashed_partial：阶段 2 预计参与部分哈希的总字节数（头尾各 4KB 上限）
- bytes_hashed_full：进入阶段 3 的文件总字节数（全量哈希）
- time_stage{1,2,3}_secs：各阶段耗时（秒）
- time_total_secs：端到端总耗时（秒）

## 性能建议

- 将扫描目录放在同一物理盘上可以减少寻道开销
- 在 SSD 上性能更佳；HDD 大量随机读时建议适当减小并行度（可后续加入可配置项）
- 现在可通过 `-j/--threads` 控制并行线程数（默认使用 CPU 逻辑核心数）。例如：`-j 8`
- 排除系统目录或不必要的挂载点能显著减少扫描时间

## 注意事项

- 本工具会跳过大小为 0 的文件
- Windows 路径建议使用双反斜杠或用引号包裹
- 对非常大的网络盘/远程目录，I/O 延迟可能成为瓶颈

## 静态链接版本

发布工作流会额外生成：

- Linux（musl 静态）：`dedup-rs-linux-musl-static.zip`
- Windows（静态 CRT）：`dedup-rs-windows-static.zip`

获取方式：在打 tag（形如 `v0.1.0`）后，前往 GitHub Releases 页面下载对应平台压缩包，解压后即可直接运行。

说明：

- Linux 静态版本针对 `x86_64-unknown-linux-musl` 目标构建，适配面更广。
- Windows 静态 CRT 版本通过 `-C target-feature=+crt-static` 链接静态运行时，便于分发。

## 许可

本项目采用双许可证：MIT OR Apache-2.0。

