import time
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage

SYSTEM_PROMPT = """你是运维 AI Agent，负责告警根因诊断。

诊断流程：
1. 分析告警信息，形成初始假设
2. 调用工具（search_logs/get_topology/analyze_blast_radius）验证假设
3. 根据工具返回的证据评估假设置信度
4. 若置信度不足，修正假设并继续验证
5. 根因确认后，输出结构化诊断报告

报告格式：
- 根因：[根因描述]
- 置信度：[0-100%]
- 证据：[关键证据列表]
- 建议：[修复建议]
"""

MAX_PROTECTED_DATA = 20
DEFAULT_MAX_MESSAGES = 50
DEFAULT_RECENT_ROUNDS = 5


class ContextManager:
    """分层上下文管理器

    Layer 1: 系统 Prompt（永远保留）
    Layer 2: protected_data（不可压缩，滑动窗口 20 条）
    Layer 3: compressed_memory（可压缩的已排除假设摘要）
    Layer 4: 当前窗口（最近 N 轮对话完整保留）
    """

    def __init__(self, max_messages: int = DEFAULT_MAX_MESSAGES,
                 recent_rounds: int = DEFAULT_RECENT_ROUNDS):
        self.max_messages = max_messages
        self.recent_rounds = recent_rounds
        self._protected: list[dict] = []
        self._compressed: list[str] = []
        self._reasoning_history: list[BaseMessage] = []

    def add_protected(self, data: dict):
        """添加关键数据到保护层"""
        self._protected.append({
            "data": data.get("data", ""),
            "tool": data.get("tool", "unknown"),
            "timestamp": data.get("timestamp", time.time()),
            "critical": data.get("critical", False),
        })
        if len(self._protected) > MAX_PROTECTED_DATA:
            self._protected = self._protected[-MAX_PROTECTED_DATA:]

    def record_reasoning_step(self, message: BaseMessage):
        """记录一步推理"""
        self._reasoning_history.append(message)

    def compress_excluded_hypothesis(self, summary: str):
        """将已排除的假设压缩为摘要"""
        self._compressed.append(summary)

    def get_recent_history(self) -> list[BaseMessage]:
        """获取最近 N 轮对话"""
        return self._reasoning_history[-self.recent_rounds * 2:]

    def build_messages(self, alert: dict,
                       reasoning_history: list[BaseMessage] = None) -> tuple[list[BaseMessage], list[dict]]:
        """构建发送给 LLM 的完整消息列表"""
        messages = []

        # Layer 1: 系统 Prompt
        messages.append(SystemMessage(content=SYSTEM_PROMPT))

        # Layer 3: 压缩摘要（已排除假设）
        if self._compressed:
            compressed_text = "\n".join(f"- {c}" for c in self._compressed[-5:])
            messages.append(SystemMessage(
                content=f"[已排除的假设]\n{compressed_text}"
            ))

        # Layer 2: protected_data
        if self._protected:
            protected_text = "\n".join(
                f"[{p['tool']}] {str(p['data'])[:200]}"
                for p in self._protected[-10:]
            )
            messages.append(SystemMessage(
                content=f"[关键证据]\n{protected_text}"
            ))

        # 告警信息
        alert_text = (
            f"告警信息：\n"
            f"- 服务：{alert.get('labels', {}).get('service', 'unknown')}\n"
            f"- 严重级别：{alert.get('severity', 'unknown')}\n"
            f"- 摘要：{alert.get('annotations', {}).get('summary', '')}\n"
            f"- 告警 ID：{alert.get('alert_id', 'unknown')}"
        )
        messages.append(HumanMessage(content=alert_text))

        # Layer 4: 当前窗口
        if reasoning_history:
            messages.extend(reasoning_history)

        # 检查是否需要压缩
        total = len(messages)
        if total > self.max_messages:
            keep = self.recent_rounds * 2 + 4
            if total > keep:
                excess = total - keep
                compress_msg = SystemMessage(
                    content=f"[上下文压缩：省略了 {excess} 条历史消息]"
                )
                insert_pos = 2
                if self._compressed:
                    insert_pos += 1
                if self._protected:
                    insert_pos += 1
                insert_pos += 1  # alert
                messages.insert(insert_pos, compress_msg)
                messages = messages[:insert_pos + 1] + messages[-(keep - insert_pos - 1):]

        return messages, self._protected

    def clear(self):
        """重置上下文"""
        self._protected = []
        self._compressed = []
        self._reasoning_history = []
