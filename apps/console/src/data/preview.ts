import type { QueryRow } from "../types";

export const initialSql = `SELECT
  id,
  title,
  category,
  hybrid_score(content, embedding) AS score,
  updated_at
FROM documents
WHERE category = 'database'
ORDER BY score DESC
LIMIT 5;`;

export const previewRows: QueryRow[] = [
  {
    id: 1042,
    title: "向量检索在事务系统中的边界",
    category: "database",
    score: 0.982,
    updatedAt: "2026-07-24 15:42",
  },
  {
    id: 889,
    title: "Rust 查询优化器设计笔记",
    category: "database",
    score: 0.947,
    updatedAt: "2026-07-24 14:18",
  },
  {
    id: 731,
    title: "WAL 与检查点恢复实践",
    category: "database",
    score: 0.921,
    updatedAt: "2026-07-23 22:06",
  },
  {
    id: 428,
    title: "混合检索成本模型",
    category: "database",
    score: 0.896,
    updatedAt: "2026-07-23 18:34",
  },
  {
    id: 216,
    title: "面向 Arrow 的批处理执行器",
    category: "database",
    score: 0.874,
    updatedAt: "2026-07-22 09:11",
  },
];

export const schemaGroups = [
  {
    name: "public",
    tables: [
      { name: "documents", count: "12.8k", kind: "table" },
      { name: "query_history", count: "846", kind: "table" },
      { name: "model_registry", count: "8", kind: "table" },
    ],
  },
  {
    name: "analytics",
    tables: [
      { name: "query_metrics", count: "24h", kind: "view" },
      { name: "index_usage", count: "7d", kind: "view" },
    ],
  },
] as const;
