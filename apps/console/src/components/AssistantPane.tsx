import {
  ArrowUpRight,
  Bot,
  Check,
  CircleGauge,
  Copy,
  Lightbulb,
  ShieldCheck,
  Sparkles,
} from "lucide-react";
import { IconAction } from "./IconAction";

const suggestions = [
  {
    icon: CircleGauge,
    title: "先过滤，再计算混合得分",
    detail: "category 条件可在向量排序前缩小候选集。",
  },
  {
    icon: ShieldCheck,
    title: "保持只读执行",
    detail: "当前语句仅包含 SELECT，适合安全预览。",
  },
] as const;

export function AssistantPane() {
  return (
    <aside className="assistant-pane" aria-label="AI 查询助手">
      <div className="assistant-heading">
        <div className="assistant-title">
          <Bot size={19} aria-hidden="true" />
          <h2>查询助手</h2>
        </div>
        <span className="assistant-mode">
          <Sparkles size={14} aria-hidden="true" />
          ADVISORY
        </span>
      </div>

      <div className="assistant-intro">
        <span className="eyebrow">当前查询</span>
        <p>这是一条带结构化过滤的混合检索查询。</p>
      </div>

      <div className="assistant-section">
        <div className="section-label">
          <Lightbulb size={16} aria-hidden="true" />
          <span>优化建议</span>
        </div>
        <div className="suggestion-list">
          {suggestions.map((suggestion) => {
            const SuggestionIcon = suggestion.icon;
            return (
              <button
                className="suggestion"
                type="button"
                key={suggestion.title}
              >
                <SuggestionIcon size={18} aria-hidden="true" />
                <span>
                  <strong>{suggestion.title}</strong>
                  <span>{suggestion.detail}</span>
                </span>
                <ArrowUpRight size={16} aria-hidden="true" />
              </button>
            );
          })}
        </div>
      </div>

      <div className="assistant-section assistant-section--plan">
        <div className="section-label">
          <CircleGauge size={16} aria-hidden="true" />
          <span>计划预览</span>
        </div>
        <ol className="plan-steps">
          <li>
            <span className="step-index">01</span>
            <span>Filter · category</span>
            <Check size={15} aria-hidden="true" />
          </li>
          <li>
            <span className="step-index">02</span>
            <span>Hybrid Scan · documents</span>
            <Check size={15} aria-hidden="true" />
          </li>
          <li>
            <span className="step-index">03</span>
            <span>TopN · 5 rows</span>
            <Check size={15} aria-hidden="true" />
          </li>
        </ol>
      </div>

      <div className="assistant-footer">
        <div>
          <span>安全建议</span>
          <strong>无需修改 SQL 语义</strong>
        </div>
        <IconAction
          label="复制优化建议"
          icon={<Copy size={16} aria-hidden="true" />}
        />
      </div>
    </aside>
  );
}
