"""采集计划执行回执查询服务：回执由调度计划触发执行自动生成，这里提供可再次核验的台账视图。

回执状态机：待确认 → 已确认 → 已认领 → 已写回。
支持按计划编号、时段、执行实例、批次编号、认领作业过滤查询，
断线恢复后已生成的回执记录仍可查。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class SchedulerReceiptService(BaseModuleService):
    MODULE = "scheduler_receipt"
    ENTITY = "采集计划执行回执"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["回执编号", "计划编号", "时段", "执行实例", "触发方式", "批次编号", "认领作业", "认领结果"]
    REQUIRED_FIELDS = ["回执编号", "计划编号", "时段"]
    STATUS_ORDER = ["待确认", "已确认", "已认领", "已写回"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def create_entry(self, values: dict[str, Any]):
        # 回执只能由调度计划触发执行生成，保证时段与执行实例可追查。
        return None, ["采集计划执行回执由调度计划触发执行自动生成，不可人工登记"]
