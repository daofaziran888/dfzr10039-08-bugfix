"""采集任务领域服务：待领列表、按采集计划执行回执认领、作业台账写回。

调度计划确认结果进入本模块待领列表（status=待执行）；采集任务按同一份
采集计划执行回执取数，领取时登记认领结果并回写作业台账，写回后回执闭环。
断线恢复后，待领与已认领记录都可照常查询，重复领取/写回自动去重。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store, store_lock

RECEIPT_MODULE = "scheduler_receipt"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _find_receipt(receipt_no: str) -> dict[str, Any] | None:
    if not receipt_no:
        return None
    for row in store.rows(RECEIPT_MODULE):
        if str(row.get("回执编号")) == str(receipt_no):
            return row
    return None


class CrawlJobService(BaseModuleService):
    MODULE = "crawl_job"
    ENTITY = "采集作业"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["作业编号", "目标站点", "入口URL", "最大深度", "并发线程", "调度策略", "完成页数", "失败页数"]
    REQUIRED_FIELDS = ["作业编号", "目标站点", "入口URL"]
    STATUS_ORDER = ["待执行", "采集执行", "已完成", "部分失败"]
    ACTION_RULES = {
        "启动采集": "采集执行",
        "领取任务": "采集执行",
        "认领任务": "采集执行",
        "写回台账": "已完成",
        "暂停作业": "已完成",
        "标记完成": "部分失败",
    }
    NEGATIVE_ACTIONS = []

    def run_action(
        self,
        entry_id: int,
        action: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        params = params or {}
        if action in ("领取任务", "认领任务"):
            return self._claim(entry, params)
        if action == "写回台账":
            return self._writeback(entry, params)
        if action == "启动采集" and entry.get("回执编号"):
            # 调度计划下派的作业走认领链路，保证台账与回执一致
            return self._claim(entry, params)
        return super().run_action(entry_id, action, params)

    def _claim(self, entry: dict[str, Any], params: dict[str, Any]):
        """领取待运行项：登记认领结果并回写作业台账，同时回写回执为已认领。"""
        with store_lock:
            if entry.get("status") == "待执行":
                claimer = str(params.get("认领人") or params.get("认领节点") or "调度worker")
                result = str(params.get("认领结果") or "认领成功")
                now = _now()
                entry["status"] = "采集执行"
                entry["pending"] = True
                entry["abnormal"] = False
                entry["认领结果"] = result
                entry["认领时间"] = now
                entry["认领人"] = claimer
                receipt = _find_receipt(str(entry.get("回执编号") or ""))
                if receipt is not None and receipt.get("status") in ("待确认", "已确认"):
                    receipt["status"] = "已认领"
                    receipt["认领作业"] = str(entry.get("作业编号") or "")
                    receipt["认领结果"] = result
                    receipt["认领时间"] = now
                receipt_no = entry.get("回执编号") or "无"
                return entry, (
                    f"采集作业已按采集计划执行回执{receipt_no}取数，"
                    f"登记认领结果「{result}」并回写作业台账"
                )
            if entry.get("status") == "采集执行" and entry.get("认领结果"):
                return entry, (
                    f"采集作业{entry.get('作业编号')}已认领（同一时段重复领取去重），"
                    f"认领结果「{entry.get('认领结果')}」已在作业台账中"
                )
            return None, f"采集作业当前状态为{entry.get('status')}，不在待领列表，不能领取"

    def _writeback(self, entry: dict[str, Any], params: dict[str, Any]):
        """写回作业台账：采集执行中的作业完成闭环，回执转为已写回。"""
        with store_lock:
            receipt = _find_receipt(str(entry.get("回执编号") or ""))
            if (
                entry.get("status") == "已完成"
                and receipt is not None
                and receipt.get("status") == "已写回"
            ):
                return entry, "作业台账已写回，采集计划执行回执已闭环，重复写回去重"
            if entry.get("status") != "采集执行":
                return None, f"采集作业当前状态为{entry.get('status')}，只有采集执行中的作业才能写回台账"
            now = _now()
            entry["status"] = "已完成"
            entry["pending"] = False
            entry["abnormal"] = False
            if params.get("完成页数") is not None and str(params.get("完成页数")).strip():
                entry["完成页数"] = str(params.get("完成页数"))
            if params.get("失败页数") is not None and str(params.get("失败页数")).strip():
                entry["失败页数"] = str(params.get("失败页数"))
            entry["写回时间"] = now
            if receipt is not None:
                receipt["status"] = "已写回"
                receipt["写回时间"] = now
                receipt["pending"] = False
            return entry, "作业台账已写回，采集计划执行回执闭环"
