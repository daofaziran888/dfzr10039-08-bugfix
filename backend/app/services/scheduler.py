"""调度编排领域服务：计划状态机、采集计划执行回执、确认环节与时段并发控制。

状态机：草稿 → 待确认 → 执行中 → 已完成；已停用为终止态。
既有记录里的「待激活」按草稿等价态兼容处理，活跃计划不受影响。

关键约定：
- 触发执行即生成「采集计划执行回执」（时段 + 执行实例 + 批次编号），
  同一计划同一时段只受理一次（人工补跑与定时触发并发到达也只生效一次），
  稍后的时段独立生成回执、独立执行。
- 计划从待确认推进到执行中，必须能凭回执追查时段和执行实例；
  确认结果进入采集任务待领列表（crawl_job，status=待执行）。
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store, store_lock

RECEIPT_MODULE = "scheduler_receipt"

# 草稿等价态：既有种子记录仍是「待激活」，活跃计划要兼容已有记录。
DRAFT_ALIASES = ("草稿", "待激活")
TERMINAL_STATUSES = ("已停用",)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_slot() -> str:
    """未显式给时段时，按当前小时兜底，保证同一小时内的触发天然去重。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00")


def _slot_tag(slot: str) -> str:
    tag = re.sub(r"[^0-9A-Za-z]+", "", str(slot))
    return tag or "MANUAL"


def _param(params: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = params.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def receipts_for(plan_entry_id: int, slot: str | None = None) -> list[dict[str, Any]]:
    """按计划行 id（必要时按时段）查执行回执，断线恢复后仍可查。"""
    rows = [
        row for row in store.rows(RECEIPT_MODULE)
        if int(row.get("计划ID", 0) or 0) == int(plan_entry_id)
    ]
    if slot is not None:
        rows = [row for row in rows if str(row.get("时段")) == str(slot)]
    rows.sort(key=lambda row: int(row.get("id", 0)))
    return rows


def find_receipt(receipt_no: str) -> dict[str, Any] | None:
    for row in store.rows(RECEIPT_MODULE):
        if str(row.get("回执编号")) == str(receipt_no):
            return row
    return None


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "调度计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划编号", "关联作业", "Cron表达式", "时区设置", "依赖检查", "失败通知", "并发控制", "下次执行"]
    REQUIRED_FIELDS = ["计划编号", "关联作业", "Cron表达式"]
    STATUS_ORDER = ["草稿", "待确认", "执行中", "已完成", "已停用"]
    ACTION_RULES = {
        "触发执行": "待确认",
        "确认执行": "执行中",
        "立即执行": "执行中",
        "激活计划": "执行中",
        "停用计划": "已停用",
    }
    NEGATIVE_ACTIONS = ["停用计划"]

    # ------------------------------------------------------------------
    # 查询：计划详情携带执行回执，供“从调度计划详情查执行回执”再次核验
    # ------------------------------------------------------------------

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(self.MODULE, entry_id)
        if row is None:
            return None
        return self._with_receipts(row)

    def _with_receipts(self, row: dict[str, Any]) -> dict[str, Any]:
        detail = dict(row)
        receipts = [dict(receipt) for receipt in receipts_for(int(row.get("id", 0)))]
        detail["执行回执"] = receipts
        detail["最新回执"] = receipts[-1] if receipts else None
        return detail

    # ------------------------------------------------------------------
    # 动作：触发 → 确认 →（采集任务认领/写回在 crawl_job 服务）
    # ------------------------------------------------------------------

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
        if action == "触发执行":
            return self._trigger(entry, params)
        if action == "确认执行":
            return self._confirm(entry, params)
        if action == "立即执行":
            return self._run_now(entry, params)
        if action == "激活计划":
            return self._activate(entry, params)
        return super().run_action(entry_id, action, params)

    def _trigger(self, entry: dict[str, Any], params: dict[str, Any]):
        """受理执行请求：生成回执并入待确认；同时段并发触发只生效一次。"""
        if entry.get("status") in TERMINAL_STATUSES:
            return None, f"{self.ENTITY}已停用，不能触发执行"
        slot = _param(params, "时段", "执行时段", "计划时段", "slot") or _default_slot()
        trigger_by = _param(params, "触发方式", "触发来源", "trigger") or "人工补跑"
        with store_lock:
            existing = receipts_for(int(entry["id"]), slot=slot)
            if existing:
                receipt = existing[0]
                message = (
                    f"受理冲突：{slot}时段已受理（同一时段并发触发只生效一次），"
                    f"沿用采集计划执行回执{receipt['回执编号']}"
                    f"（首次由{receipt.get('触发方式') or '未知来源'}受理，"
                    f"执行实例{receipt['执行实例']}）"
                )
                return self._with_receipts(entry), message
            receipt = self._new_receipt(entry, slot, trigger_by)
            if entry.get("status") in DRAFT_ALIASES + ("已完成",):
                entry["status"] = "待确认"
                entry["pending"] = True
                entry["abnormal"] = False
                state_note = "计划进入待确认"
            else:
                state_note = f"计划保持{entry.get('status')}"
            message = (
                f"已受理{slot}时段执行请求（{trigger_by}），生成采集计划执行回执"
                f"{receipt['回执编号']}，执行实例{receipt['执行实例']}，"
                f"批次{receipt['批次编号']}，{state_note}"
            )
            return self._with_receipts(entry), message

    def _confirm(self, entry: dict[str, Any], params: dict[str, Any]):
        """确认环节：凭回执追查时段与执行实例，推进到执行中并下派采集任务。"""
        slot = _param(params, "时段", "执行时段", "计划时段", "slot")
        with store_lock:
            pending = [
                receipt for receipt in receipts_for(int(entry["id"]), slot=slot or None)
                if receipt.get("status") == "待确认"
            ]
            if not pending:
                if slot:
                    existing = receipts_for(int(entry["id"]), slot=slot)
                    if existing:
                        receipt = existing[0]
                        return self._with_receipts(entry), (
                            f"{slot}时段回执{receipt['回执编号']}已确认，"
                            f"采集任务已在待领列表，重复确认去重"
                        )
                return None, (
                    "没有待确认的采集计划执行回执，计划不能从待确认推进到执行中"
                    "（需先触发执行，保留可追查的时段与执行实例）"
                )
            receipt = pending[0]
            if not receipt.get("时段") or not receipt.get("执行实例"):
                return None, "采集计划执行回执缺少时段或执行实例，无法追查，确认被拒绝"
            receipt["status"] = "已确认"
            receipt["确认时间"] = _now()
            entry["status"] = "执行中"
            entry["pending"] = True
            entry["abnormal"] = False
            job_row = self._ensure_pending_job(entry, receipt)
            message = (
                f"已确认{receipt['时段']}时段执行实例{receipt['执行实例']}，"
                f"计划进入执行中，采集任务{job_row['作业编号']}已进入待领列表"
            )
            return self._with_receipts(entry), message

    def _run_now(self, entry: dict[str, Any], params: dict[str, Any]):
        """立即执行 = 触发 + 确认一步到位；同时段依旧只生效一次。"""
        detail, message = self._trigger(entry, params)
        if detail is None:
            return None, message
        return self._confirm(entry, params)

    def _activate(self, entry: dict[str, Any], params: dict[str, Any]):
        """兼容既有记录：有待确认回执走确认链路，否则直接置为执行中。"""
        if any(receipt.get("status") == "待确认" for receipt in receipts_for(int(entry["id"]))):
            return self._confirm(entry, params)
        entry["status"] = "执行中"
        entry["pending"] = True
        entry["abnormal"] = False
        return self._with_receipts(entry), f"{self.ENTITY}已激活计划（兼容既有记录），进入执行中"

    # ------------------------------------------------------------------
    # 内部：回执生成与采集任务下派
    # ------------------------------------------------------------------

    def _new_receipt(self, entry: dict[str, Any], slot: str, trigger_by: str) -> dict[str, Any]:
        plan_code = str(entry.get("计划编号") or f"PLAN-{entry.get('id')}")
        tag = _slot_tag(slot)
        rows = store.rows(RECEIPT_MODULE)
        receipt = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "回执编号": f"RCPT-{plan_code}-{tag}",
            "计划编号": plan_code,
            "计划ID": int(entry.get("id", 0)),
            "关联作业": str(entry.get("关联作业") or ""),
            "时段": slot,
            "执行实例": f"INST-{plan_code}-{tag}",
            "触发方式": trigger_by,
            "批次编号": f"BATCH-{plan_code}-{tag}",
            "认领作业": "",
            "认领结果": "",
            "status": "待确认",
            "pending": True,
            "abnormal": False,
            "创建时间": _now(),
            "确认时间": "",
            "认领时间": "",
            "写回时间": "",
        }
        rows.append(receipt)
        return receipt

    def _ensure_pending_job(self, entry: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
        """确认结果进入采集任务待领列表；按回执编号去重，断线重确认不产生重复任务。"""
        for row in store.rows("crawl_job"):
            if str(row.get("回执编号") or "") == receipt["回执编号"]:
                return row
        linked: dict[str, Any] | None = None
        for row in store.rows("crawl_job"):
            if str(row.get("作业编号")) == str(entry.get("关联作业") or ""):
                linked = row
                break
        tag = _slot_tag(receipt["时段"])
        rows = store.rows("crawl_job")
        job_row = {
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "作业编号": f"JOB-{receipt['计划编号']}-{tag}",
            "目标站点": str((linked or {}).get("目标站点") or entry.get("关联作业") or ""),
            "入口URL": str((linked or {}).get("入口URL") or ""),
            "最大深度": str((linked or {}).get("最大深度") or "3"),
            "并发线程": str((linked or {}).get("并发线程") or "4"),
            "调度策略": "调度计划",
            "完成页数": "0",
            "失败页数": "0",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "计划编号": receipt["计划编号"],
            "回执编号": receipt["回执编号"],
            "时段": receipt["时段"],
            "执行实例": receipt["执行实例"],
            "批次编号": receipt["批次编号"],
            "认领结果": "",
            "认领时间": "",
            "认领人": "",
        }
        rows.append(job_row)
        return job_row

    # ------------------------------------------------------------------
    # 快照：既有「待激活」记录并入草稿统计，活跃计划兼容已有记录
    # ------------------------------------------------------------------

    def domain_snapshot(self) -> dict[str, Any]:
        snap = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        legacy = sum(1 for row in rows if row.get("status") == "待激活")
        if legacy:
            snap["by_status"]["草稿"] = snap["by_status"].get("草稿", 0) + legacy
        snap["回执数"] = len(store.rows(RECEIPT_MODULE))
        return snap
