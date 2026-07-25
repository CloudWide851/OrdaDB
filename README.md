<p align="center">
  <img src="./logo.svg" width="88" height="88" alt="OrdaDB logo" />
</p>

# OrdaDB

OrdaDB 是一个以 Rust 构建的 AI 原生混合型关系数据库项目。仓库目前同时包含可运行的 Windows x64 桌面工作台，以及正在按独立里程碑演进的持久化关系数据库内核。

## 当前能力

- React、TypeScript、Vite 和 pnpm 管理端
- Monaco SQL 编辑器、Schema 浏览器、结果/日志视图和 AI 建议面板
- Tauri 2 Windows 桌面壳与最小 Rust 状态桥接
- macOS 风格的浅色玻璃层次、标准 SVG 图标、tooltip 和键盘操作
- Rust 2024 内核工作区、PostgreSQL 方言解析/绑定和结构化查询事件
- v1 8 KiB 校验和页面、Heap、Buffer Pool、Catalog 与索引快照持久化
- 主键、唯一、复合、覆盖 B+Tree 索引和统计信息驱动的基础成本规划
- 单表 CRUD、INNER/LEFT JOIN、分组聚合、HAVING、排序、限制和 `EXPLAIN`

Console 的查询区域目前仍明确使用预览 fixture，尚未连接数据库内核；真实 Console 集成属于后续里程碑。

## 开发

要求 Node.js 22、pnpm 10、Rust 1.89、MSVC x64 构建工具和 WebView2。

```powershell
pnpm install --frozen-lockfile
pnpm dev
pnpm desktop:dev
```

## 验证

```powershell
pnpm lint
pnpm typecheck
pnpm test
pnpm test:e2e
pnpm build

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --target x86_64-pc-windows-msvc -- -D warnings
cargo test --workspace --target x86_64-pc-windows-msvc
```

## Windows x64 安装包

```powershell
pnpm desktop:build:x64
```

桌面打包仅启用 NSIS EXE，不生成 MSI、AppX 或其他操作系统产物。

## 项目阶段

当前内核仍使用提交前完整候选快照和单写冲突检测，不宣称 Read Committed、WAL、检查点或崩溃恢复。PostgreSQL Wire、Windows 服务/CLI、广泛 DDL/PLpgSQL、全文/向量检索、真实 Console 集成与 AI 推理属于后续里程碑。
