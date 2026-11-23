"""调度计划执行回执链路测试：草稿触发 → 待确认 → 确认 → 待领 → 认领 → 写回。

覆盖任务书场景：
- 修复“编排显示执行中、重读仍是草稿”的状态回写；
- 计划 P8 送出执行请求后断线，同时段人工补跑与定时触发只生效一次；
- 待确认推进到执行中前必须能追查时段和执行实例；
- 确认结果进入采集任务待领列表，按同一份回执认领并回写作业台账；
- 断在接收作业与写回之间，恢复后待定记录仍可查；
- 相同/下一个时段各发请求，查到实际认领的采集作业和批次。
"""
from __future__ import annotations

from app.integration import schedule_job
from app.models import CrawlJobSpec, SiteConfig, SiteType
from app.services import service_for
from app.services.crawl_job import CrawlJobService
from app.services.scheduler import SchedulerService, receipts_for
from app.store import store

scheduler = SchedulerService()
crawl_job = CrawlJobService()

SLOT_A = "2099-01-01T08:00"
SLOT_B = "2099-01-01T09:00"


def _make_plan(plan_code: str, linked_job: str = "CRAW-0001") -> dict:
    entry, missing = scheduler.create_entry({
        "计划编号": plan_code,
        "关联作业": linked_job,
        "Cron表达式": "0 * * * *",
        "时区设置": "Asia/Shanghai",
        "依赖检查": "",
        "失败通知": "",
        "并发控制": "",
        "下次执行": "",
    })
    assert not missing, f"登记调度计划不应缺字段: {missing}"
    assert entry is not None
    return entry


def _pending_jobs(receipt_no: str | None = None) -> list[dict]:
    rows, _ = crawl_job.list_entries(status="待执行", size=1000)
    if receipt_no:
        rows = [row for row in rows if row.get("回执编号") == receipt_no]
    return rows


# ---------------------------------------------------------------------------
# 主流程：草稿 P8 触发一次 → 详情查回执 → 确认 → 待领 → 认领 → 写回
# ---------------------------------------------------------------------------

def test_p8_full_receipt_flow() -> None:
    """草稿 P8 完整走一遍：触发、详情查回执、确认、待领、认领、写回。"""
    plan = _make_plan("SCHE-P8-E2E")
    assert plan["status"] == "草稿", f"新计划应为草稿，实际: {plan['status']}"

    # 1. 用草稿 P8 触发一次 → 进入待确认，生成执行回执
    detail, message = scheduler.run_action(
        plan["id"], "触发执行",
        params={"时段": SLOT_A, "触发方式": "定时触发"},
    )
    assert detail is not None, f"触发应受理: {message}"
    assert detail["status"] == "待确认", f"触发后应进入待确认，实际: {detail['status']}"

    # 2. 从调度计划详情查执行回执（可再次核验）
    detail = scheduler.get_entry(plan["id"])
    receipts = detail["执行回执"]
    assert len(receipts) == 1, f"应有1条回执，实际: {len(receipts)}"
    receipt = receipts[0]
    assert receipt["时段"] == SLOT_A, "回执应记录时段"
    assert receipt["执行实例"], "回执应记录执行实例"
    assert receipt["批次编号"], "回执应记录批次编号"
    assert receipt["status"] == "待确认"

    # 3. 确认执行 → 计划进入执行中，确认结果进入采集任务待领列表
    detail, message = scheduler.run_action(plan["id"], "确认执行", params={"时段": SLOT_A})
    assert detail is not None, f"确认应成功: {message}"
    assert detail["status"] == "执行中", f"确认后应进入执行中，实际: {detail['status']}"
    waiting = _pending_jobs(receipt["回执编号"])
    assert len(waiting) == 1, f"待领列表应有1条任务，实际: {len(waiting)}"
    task = waiting[0]
    assert task["时段"] == SLOT_A and task["执行实例"] == receipt["执行实例"], \
        "采集任务应按同一份回执取数"

    # 4. 打开采集任务列表领取待运行项 → 登记认领结果并回写作业台账
    claimed, message = crawl_job.run_action(task["id"], "领取任务", params={"认领人": "worker-1"})
    assert claimed is not None, f"领取应成功: {message}"
    assert claimed["status"] == "采集执行"
    assert claimed["认领结果"] == "认领成功", "台账应登记认领结果"
    assert claimed["认领人"] == "worker-1"

    # 5. 写回台账 → 回执闭环
    done, message = crawl_job.run_action(task["id"], "写回台账", params={"完成页数": "12"})
    assert done is not None, f"写回应成功: {message}"
    assert done["status"] == "已完成"
    receipt_after = [r for r in receipts_for(plan["id"]) if r["回执编号"] == receipt["回执编号"]][0]
    assert receipt_after["status"] == "已写回", f"回执应闭环为已写回，实际: {receipt_after['status']}"
    assert receipt_after["认领作业"] == task["作业编号"]


def test_same_slot_concurrent_trigger_only_once() -> None:
    """人工补跑和定时触发同时到达同一时段：受理冲突，只生效一次。"""
    plan = _make_plan("SCHE-P8-CONFLICT")

    first, _ = scheduler.run_action(plan["id"], "触发执行",
                                    params={"时段": SLOT_A, "触发方式": "定时触发"})
    assert first is not None
    # 断线后人工补跑，同一时段再次到达
    second, message = scheduler.run_action(plan["id"], "触发执行",
                                           params={"时段": SLOT_A, "触发方式": "人工补跑"})
    assert second is not None, "重复触发应幂等返回已受理回执"
    assert "冲突" in message or "只生效一次" in message, f"应提示受理冲突: {message}"

    receipts = receipts_for(plan["id"], slot=SLOT_A)
    assert len(receipts) == 1, f"同一时段只能受理一次，实际回执数: {len(receipts)}"
    instances = {receipt["执行实例"] for receipt in receipts}
    assert len(instances) == 1, "同一时段只能有一个执行实例"


def test_later_slot_runs_independently() -> None:
    """稍后的时段仍可独立执行：各自生成回执、各自确认下派。"""
    plan = _make_plan("SCHE-P8-NEXT-SLOT")

    scheduler.run_action(plan["id"], "触发执行", params={"时段": SLOT_A})
    scheduler.run_action(plan["id"], "触发执行", params={"时段": SLOT_B})

    receipts = receipts_for(plan["id"])
    slots = {receipt["时段"] for receipt in receipts}
    assert slots == {SLOT_A, SLOT_B}, f"两个时段应各自独立，实际: {slots}"

    # 分别确认，各自进入待领列表
    scheduler.run_action(plan["id"], "确认执行", params={"时段": SLOT_A})
    scheduler.run_action(plan["id"], "确认执行", params={"时段": SLOT_B})
    batches = {receipt["批次编号"] for receipt in receipts_for(plan["id"])}
    assert len(batches) == 2, "每个时段应有独立批次"
    waiting_a = _pending_jobs([r for r in receipts_for(plan["id"]) if r["时段"] == SLOT_A][0]["回执编号"])
    waiting_b = _pending_jobs([r for r in receipts_for(plan["id"]) if r["时段"] == SLOT_B][0]["回执编号"])
    assert len(waiting_a) == 1 and len(waiting_b) == 1, "两个时段的任务应各自独立待领"


def test_confirm_requires_traceable_receipt() -> None:
    """计划从待确认推进到执行中前，必须能凭回执追查时段和执行实例。"""
    plan = _make_plan("SCHE-P8-NO-RECEIPT")
    entry, message = scheduler.run_action(plan["id"], "确认执行")
    assert entry is None, "没有回执时不能确认"
    assert "回执" in message and ("时段" in message or "执行实例" in message), \
        f"失败信息应指出需要可追查的回执: {message}"
    assert store.find("scheduler", plan["id"])["status"] == "草稿", "状态不应被推进"


def test_disconnect_between_claim_and_writeback() -> None:
    """断在接收作业与写回之间：恢复后待定记录可查，重复领取去重，回执不丢。"""
    plan = _make_plan("SCHE-P8-DISCONNECT")
    scheduler.run_action(plan["id"], "触发执行", params={"时段": SLOT_A})
    scheduler.run_action(plan["id"], "确认执行", params={"时段": SLOT_A})
    receipt = receipts_for(plan["id"], slot=SLOT_A)[0]
    task = _pending_jobs(receipt["回执编号"])[0]

    # 接收作业（领取成功）后断线，尚未写回台账
    claimed, _ = crawl_job.run_action(task["id"], "领取任务", params={"认领人": "worker-7"})
    assert claimed is not None

    # 断线恢复：待定记录（其它待执行任务）仍可查，回执仍可查
    still_waiting = _pending_jobs()
    assert any(row["作业编号"] == "CRAW-0001" for row in still_waiting), \
        "已有待定记录在断线恢复后应仍能查"
    recovered = receipts_for(plan["id"], slot=SLOT_A)
    assert len(recovered) == 1 and recovered[0]["status"] == "已认领", \
        "已认领的回执在恢复后应仍可查"

    # 恢复后重复领取：去重，不重复登记
    again, message = crawl_job.run_action(task["id"], "领取任务", params={"认领人": "worker-8"})
    assert again is not None
    assert again["认领人"] == "worker-7", "重复领取应保留首次认领结果"
    assert "去重" in message or "已认领" in message

    # 同时段重发触发请求仍只生效一次；下一个时段独立受理
    scheduler.run_action(plan["id"], "触发执行", params={"时段": SLOT_A, "触发方式": "人工补跑"})
    assert len(receipts_for(plan["id"], slot=SLOT_A)) == 1, "同时段重发不应产生新回执"
    scheduler.run_action(plan["id"], "触发执行", params={"时段": SLOT_B})
    assert len(receipts_for(plan["id"], slot=SLOT_B)) == 1, "下一时段应独立受理"

    # 查到实际认领的采集作业和批次
    running, _ = crawl_job.list_entries(status="采集执行", size=1000)
    mine = [row for row in running if row.get("回执编号") == receipt["回执编号"]]
    assert len(mine) == 1, "应查到实际认领的采集作业"
    assert mine[0]["批次编号"] == receipt["批次编号"], "作业应可追到批次"


def test_legacy_plan_records_compatible() -> None:
    """活跃计划兼容已有记录：待激活旧记录可触发，执行中旧记录可开新时段。"""
    store.rows("scheduler").append({
        "id": 881, "计划编号": "SCHE-LEGACY-IDLE", "关联作业": "CRAW-0002",
        "Cron表达式": "0 */6 * * *", "时区设置": "Asia/Shanghai",
        "依赖检查": "", "失败通知": "", "并发控制": "", "下次执行": "",
        "status": "待激活", "pending": True, "abnormal": False,
    })
    store.rows("scheduler").append({
        "id": 882, "计划编号": "SCHE-LEGACY-RUNNING", "关联作业": "CRAW-0003",
        "Cron表达式": "0 */6 * * *", "时区设置": "Asia/Shanghai",
        "依赖检查": "", "失败通知": "", "并发控制": "", "下次执行": "",
        "status": "执行中", "pending": True, "abnormal": False,
    })

    # 待激活旧记录：按草稿等价态触发
    detail, message = scheduler.run_action(881, "触发执行", params={"时段": SLOT_A})
    assert detail is not None, f"旧记录触发应兼容: {message}"
    assert detail["status"] == "待确认"
    detail, _ = scheduler.run_action(881, "确认执行", params={"时段": SLOT_A})
    assert detail is not None and detail["status"] == "执行中"

    # 执行中旧记录：新时段独立受理，计划保持执行中
    detail, _ = scheduler.run_action(882, "触发执行", params={"时段": SLOT_B})
    assert detail is not None
    assert detail["status"] == "执行中", "活跃计划开新时段不应退回待确认"
    assert len(receipts_for(882, slot=SLOT_B)) == 1

    # 旧动作“激活计划”仍可用（无回执时直接置为执行中）
    detail, _ = scheduler.run_action(881, "激活计划")
    assert detail is not None and detail["status"] == "执行中"


def test_schedule_job_persists_status_to_store() -> None:
    """修复“编排显示执行中、重读仍是草稿”：schedule_job 状态回写台账行。"""
    store.rows("scheduler").append({
        "id": 883, "计划编号": "SCHE-LEGACY-DRAFT", "关联作业": "JOB-LEDGER-1",
        "Cron表达式": "0 */6 * * *", "时区设置": "Asia/Shanghai",
        "依赖检查": "", "失败通知": "", "并发控制": "", "下次执行": "",
        "status": "待激活", "pending": True, "abnormal": False,
    })
    store.rows("crawl_job").append({
        "id": 883, "作业编号": "JOB-LEDGER-1", "目标站点": "ledger.example.com",
        "入口URL": "https://ledger.example.com", "最大深度": "3", "并发线程": "4",
        "调度策略": "interval", "完成页数": "0", "失败页数": "0",
        "status": "待执行", "pending": True, "abnormal": False,
    })
    job = CrawlJobSpec(
        job_id="JOB-LEDGER-1",
        site=SiteConfig(site_id="S9", domain="ledger.example.com", site_type=SiteType.NEWS,
                        robots_policy="allow", language="zh-CN", region="CN",
                        access_method="direct", registered_at="2026-01-01"),
        entry_urls=["https://ledger.example.com"], status="采集执行",
    )

    plan = schedule_job(job)
    assert plan is not None and plan.status == "执行中"

    # 重新读取已有计划：状态已回写，不再停留在草稿/待激活
    row = store.find("scheduler", 883)
    assert row["status"] == "执行中", f"重读计划应为执行中，实际: {row['status']}"
    job_row = store.find("crawl_job", 883)
    assert job_row["status"] == "采集执行", "关联采集作业的状态应同步回写作业台账"


def test_receipt_module_queryable() -> None:
    """回执台账可独立查询核验：按计划编号/时段/批次过滤。"""
    receipt_service = service_for("scheduler_receipt")
    assert receipt_service is not None, "回执模块应注册可查"

    plan = _make_plan("SCHE-P8-QUERY")
    scheduler.run_action(plan["id"], "触发执行", params={"时段": SLOT_A})
    receipt = receipts_for(plan["id"], slot=SLOT_A)[0]

    rows, total = receipt_service.list_entries(filters={"计划编号": "SCHE-P8-QUERY"})
    assert total == 1 and rows[0]["回执编号"] == receipt["回执编号"]
    rows, total = receipt_service.list_entries(filters={"批次编号": receipt["批次编号"]})
    assert total == 1, "应能按批次编号查到回执"
    rows, total = receipt_service.list_entries(status="待确认")
    assert total >= 1, "待确认回执应可过滤"

    # 回执不可人工登记
    entry, missing = receipt_service.create_entry({"回执编号": "RCPT-FAKE"})
    assert entry is None and missing, "回执不应允许人工登记"
