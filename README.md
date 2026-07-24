<p align="center">
  <img src="./logo.svg" width="88" height="88" alt="OrdaDB logo" />
</p>

# OrdaDB

OrdaDB 是一个以 Rust 构建的 AI 原生混合型关系数据库项目。当前仓库首先提供可运行的 Windows x64 桌面工作台，用于验证专业数据库管理界面的工程基础与交互方向。

## 当前能力

- React、TypeScript、Vite 和 pnpm 管理端
- Monaco SQL 编辑器、Schema 浏览器、结果/日志视图和 AI 建议面板
- Tauri 2 Windows 桌面壳与最小 Rust 状态桥接
- 清晰标识的本地示例查询，不会连接或伪装真实数据库内核
- macOS 风格的浅色玻璃层次、标准 SVG 图标、tooltip 和键盘操作

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

本阶段不包含数据库内核、真实 SQL 执行、存储、事务、网络协议或 AI 推理。后续内核实现将继续遵循 Rust 负责语义正确性与事务兜底、AI 负责估计与建议的边界。
