<p align="center">
  <img src="./logo.svg" width="88" height="88" alt="OrdaDB logo" />
</p>

# OrdaDB

OrdaDB 是一个以 Rust 构建的单机持久化关系数据库与 Windows SQL
工作台。PostgreSQL 是内部语义标准；MySQL、SQLite 和 SQL Server 输入只在可验证
范围内归一化到 OrdaDB AST，不宣称完整兼容四套厂商语义。

## 当前能力

- 8 KiB 校验和页面、Heap、Buffer Pool、B+Tree、WAL、检查点和启动恢复
- 单写多读 Read Committed、Undo、Commit/Rollback 和结构化 SQL 错误
- PostgreSQL v3、SCRAM-SHA-256、RBAC、Simple/Extended Query、取消和 COPY
- 常用关系 SQL、DDL、PL/pgSQL 子集、触发器、全文索引、VECTOR/HNSW 和混合检索
- Axum 管理 API、Windows LocalService 服务、机器可读 CLI
- 一致性逻辑归档 v1、原子恢复、CSV/JSON Lines 原子导入导出
- React、Monaco 与 Tauri 2 Console：独立 OrdaDB/PostgreSQL 数据源、六阶段
  本地诊断、对象树、查询流、事务、监控、检查点和作业管理
- SQL 工作台支持未命名/工作区/外部文件、首次保存时 Save As、拖放、最近文件、
  冲突恢复、命令注册表快捷键，以及可搜索的六类 `ConsoleSettingsV2`
- 官方签名的 Windows x64 隔离连接插件；未配置 Registry 时默认拒绝下载

浏览器开发预览始终显示 `Preview fixture`，不会伪装真实数据库、文件写入或服务操作。
AI 面板仍是后续入口，不执行模型推理。

数据库密码由 Rust 调用 Windows 原生安全提示并直接写入 Credential Manager；密码
不会进入 React、Zustand、Tauri 请求 DTO、日志或 LocalAppData 状态文件。结果分页、
驻留内存、NULL 显示、查询/连接超时、自动保存和危险写入确认均由已验证的设置驱动。

## 开发环境

要求 Windows x64、Node.js 22、pnpm 10、Rust 1.89、MSVC x64 构建工具和
WebView2。

```powershell
pnpm install --frozen-lockfile
pnpm dev
pnpm desktop:dev
```

数据库服务默认监听：

- PostgreSQL：`127.0.0.1:54329`
- 管理 API：`127.0.0.1:9080`
- 数据目录：`%ProgramData%\OrdaDB\data`

远程监听必须显式配置 Rustls 证书和私钥。

## 严格验证

```powershell
pnpm install --frozen-lockfile
pnpm lint
pnpm typecheck
pnpm test
pnpm test:e2e
pnpm build

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --target x86_64-pc-windows-msvc -- -D warnings
cargo test --workspace --target x86_64-pc-windows-msvc
cargo build --workspace --release --target x86_64-pc-windows-msvc
cargo deny check
cargo audit
cargo llvm-cov --no-clean -p <affected-crate> --target x86_64-pc-windows-msvc --summary-only -- --test-threads=1
```

Cargo 检查必须串行运行；Playwright 应独占运行，避免与 release/installer 构建争用。
覆盖率按受影响 crate 拆分，不在此 Windows 主机运行整 workspace 聚合插桩。

## 最终规模门禁

规模门禁是独立的 Windows x64 Rust example，不属于安装布局，因此不会增加第四个交付
程序。Smoke profile 会创建一次性数据目录、验证紧凑 v1 页目录、流式逐行校验、查询
内存峰值、多连接与单 writer 排他性，写出机器可读 JSON 后清理测试数据：

```powershell
pnpm scale:smoke:x64
```

完整门禁固定为 20 GiB 数据文件、1000 万行、32 个同时连接和一个 writer。它要求
显式确认，并在启动前检查至少 60 GiB 可用磁盘及 80 GiB 可用物理内存；证据和数据
会保留以便审计：

```powershell
pnpm scale:full:x64
```

JSON 证据默认写入 `target\final-scale\evidence`，包含实际文件大小、行数与 ID
校验和、执行器计量的峰值内存、连接/第二 writer 结果、恢复状态及各阶段耗时。完整
profile 不接受缩小目标的覆盖参数；资源不足时会在创建数据前明确拒绝，并写出包含
可用/所需资源的 failed JSON，不会把 Smoke 结果标成完整门禁。

## Windows x64 安装包

```powershell
pnpm desktop:build:x64
```

该命令先构建并验证 AMD64 Server/CLI，再由 Tauri 生成唯一的 per-machine NSIS：

```text
target\x86_64-pc-windows-msvc\release\bundle\nsis\OrdaDB_0.1.0_x64-setup.exe
```

安装目录包含且只交付三个主程序：

- `ordadb.exe` — Console
- `ordadb-server.exe` — PostgreSQL/管理 API 与 Windows 服务
- `ordadb-cli.exe` — SQL、管理与数据操作 CLI

安装器注册 `OrdaDB` own-process 服务，使用 `NT AUTHORITY\LocalService`、延迟自动
启动及 5/15/60 秒失败重启策略。升级会幂等更新并重启服务；默认卸载删除程序与服务
注册，但保留 `%ProgramData%\OrdaDB\data`。删除数据必须由管理员另行显式执行。

服务也可通过同一个 Rust 状态机管理：

```powershell
.\ordadb-server.exe service status
.\ordadb-server.exe service install
.\ordadb-server.exe service start
.\ordadb-server.exe service stop
.\ordadb-server.exe service uninstall
```

这些命令需要管理员权限；重复启停、安装或卸载是安全的。

## 备份、恢复与文件交换

原生文件操作只接受服务 operations root 下的相对路径。密码只能从 stdin 进入 CLI，
禁止放入命令行参数。

```powershell
$credential = Get-Credential -UserName dba
$credential.GetNetworkCredential().Password |
  .\ordadb-cli.exe backup --user dba --password-stdin --path nightly.ordbak

$credential.GetNetworkCredential().Password |
  .\ordadb-cli.exe operations --user dba --password-stdin

$credential.GetNetworkCredential().Password |
  .\ordadb-cli.exe restore --user dba --password-stdin --path nightly.ordbak

$credential.GetNetworkCredential().Password |
  .\ordadb-cli.exe export --user dba --password-stdin `
    --schema public --table documents --format csv --path documents.csv

$credential.GetNetworkCredential().Password |
  .\ordadb-cli.exe import --user dba --password-stdin `
    --schema public --table documents --format json-lines --path documents.jsonl
```

逻辑归档使用 `ORDBAK01`/v1、长度边界与 SHA-256。恢复先在候选目录完整验证，
再替换活动数据并保留可回滚边界；B+Tree、Tantivy 和 HNSW 从 Catalog 与权威行重建。
损坏、截断、未知版本、越界输入或失败导入不会发布部分状态。

## 兼容性边界

- 支持的是 PostgreSQL 与 DataGrip 的可验证子集，不是完整 PostgreSQL 实现。
- 事务是单 writer Read Committed，不是 MVCC、SSI 或分布式事务。
- 不提供复制、物理备份、WAL 归档、PITR、任意第三方插件或 AI 模型推理。
- MySQL、SQLite、SQL Server 方言只处理可靠的引号、参数、分页、类型别名和常用
  DDL；无法归一化的厂商特性返回 `0A000`。
- 数据文件和逻辑归档均从 v1 开始；未知格式版本拒绝打开，不静默迁移或修复。
