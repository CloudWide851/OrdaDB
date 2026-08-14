import { FileClock, RotateCcw, Trash2 } from "lucide-react";
import { useRef } from "react";
import { motionDurations, usePresence } from "../lib/motion";
import { useWorkbenchStore } from "../store/workbench";

interface WorkspaceRecoveryProps {
  open: boolean;
  onRestore: () => void;
  onDiscard: () => void;
}

export function WorkspaceRecovery({
  open,
  onRestore,
  onDiscard,
}: WorkspaceRecoveryProps) {
  const recovery = useWorkbenchStore((state) => state.recovery);
  const lastRecoveryRef = useRef(recovery);
  if (recovery) lastRecoveryRef.current = recovery;
  const renderedRecovery = recovery ?? lastRecoveryRef.current;
  const presence = usePresence(Boolean(open && recovery), {
    enterDurationMs: motionDurations.feedback,
    exitDurationMs: motionDurations.exitFeedback,
  });
  if (!presence.mounted || !renderedRecovery) return null;

  return (
    <div
      className="recovery-banner"
      data-motion-presence="feedback"
      data-motion-state={presence.phase}
      role="status"
      aria-label="可恢复的 SQL 草稿"
    >
      <FileClock size={16} aria-hidden="true" />
      <div>
        <strong>发现上次 SQL 草稿</strong>
        <span>{renderedRecovery.openDocuments.length} 个文件，等待显式恢复</span>
      </div>
      <button className="secondary-action" type="button" onClick={onDiscard}>
        <Trash2 size={14} aria-hidden="true" />
        丢弃
      </button>
      <button className="primary-action" type="button" onClick={onRestore}>
        <RotateCcw size={14} aria-hidden="true" />
        恢复
      </button>
    </div>
  );
}
