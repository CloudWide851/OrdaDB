import {
  Braces,
  Database,
  Eye,
  KeyRound,
  Layers3,
  ListOrdered,
  Table2,
  Workflow,
  Zap,
  type LucideIcon,
} from "lucide-react";

export interface DatabaseObjectGroup {
  id: string;
  label: string;
  count: number;
  icon: LucideIcon;
  objects: string[];
}

export const databaseObjectGroups: DatabaseObjectGroup[] = [
  { id: "tables", label: "表", count: 4, icon: Table2, objects: ["documents", "events"] },
  { id: "views", label: "视图", count: 2, icon: Eye, objects: ["active_documents"] },
  {
    id: "materialized-views",
    label: "物化视图",
    count: 1,
    icon: Layers3,
    objects: ["document_metrics"],
  },
  {
    id: "sequences",
    label: "序列",
    count: 2,
    icon: ListOrdered,
    objects: ["documents_id_seq"],
  },
  {
    id: "indexes",
    label: "索引",
    count: 5,
    icon: KeyRound,
    objects: ["documents_pkey", "documents_search_idx"],
  },
  {
    id: "functions",
    label: "函数",
    count: 3,
    icon: Braces,
    objects: ["search_documents"],
  },
  {
    id: "procedures",
    label: "过程",
    count: 1,
    icon: Workflow,
    objects: ["refresh_document_metrics"],
  },
  {
    id: "triggers",
    label: "触发器",
    count: 2,
    icon: Zap,
    objects: ["documents_updated_at"],
  },
];

export const databaseSummary = {
  connection: "OrdaDB Local",
  database: "ordadb",
  schema: "public",
  icon: Database,
};
