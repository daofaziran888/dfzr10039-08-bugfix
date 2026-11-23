# 虫巢工坊 · 分布式爬虫调度与反爬对抗平台

面向大规模网页采集的分布式爬虫调度、代理池管理、反爬策略对抗、内容抽取清洗与采集结果交付平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集站点 | `crawl_site` | 目标站点 | 站点编号、站点域名、站点类型 |
| 代理池 | `proxy_pool` | 代理节点 | 节点编号、代理地址、代理类型 |
| 反爬对抗 | `anti_block` | 对抗策略 | 策略编号、目标站点、对抗类型 |
| 采集任务 | `crawl_job` | 采集作业 | 作业编号、目标站点、入口URL |
| URL去重 | `url_dedup` | 去重指纹 | 指纹编号、源URL、指纹算法 |
| 内容抽取 | `content_extract` | 抽取规则 | 规则编号、目标站点、XPath选择器 |
| 字段清洗 | `field_clean` | 清洗规则 | 规则编号、目标字段、清洗类型 |
| 批次存储 | `batch_store` | 采集批次 | 批次编号、来源作业、数据行数 |
| 增量采集 | `incr_crawl` | 增量标记 | 标记编号、目标站点、增量字段 |
| 解析日志 | `parse_log` | 解析记录 | 记录编号、来源URL、解析规则 |
| 数据投递 | `data_delivery` | 投递任务 | 任务编号、数据来源、目标系统 |
| 质量抽样 | `quality_sample` | 抽样计划 | 计划编号、目标批次、抽样比例 |
| 站点监控 | `site_monitor` | 监控指标 | 指标编号、目标站点、监控类型 |
| 验证码识别 | `robot_verify` | 识别记录 | 记录编号、目标站点、验证码类型 |
| 采集限速 | `crawl_limit` | 限速策略 | 策略编号、目标站点、请求间隔 |
| 模板库 | `template_lib` | 采集模板 | 模板编号、站点类型、模板名称 |
| 调度编排 | `scheduler` | 调度计划 | 计划编号、关联作业、Cron表达式 |
| 采集计费 | `crawl_cost` | 用量记录 | 记录编号、采集站点、计费周期 |
| 数据归档 | `data_archive` | 归档策略 | 策略编号、数据范围、归档周期 |
| 差异检测 | `diff_check` | 差异记录 | 记录编号、目标站点、检测页面 |
| 导出任务 | `export_task` | 导出记录 | 记录编号、数据来源、导出格式 |
| 操作审计 | `audit_log` | 审计记录 | 记录编号、操作模块、操作人 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 调度执行回执流（scheduler → crawl_job）

调度计划状态机：`草稿 → 待确认 → 执行中 → 已完成/已停用`；既有 `待激活`
记录按草稿等价态兼容。动作接口支持 `params` 传参（时段、触发方式、认领人等）。

- `POST /api/scheduler/{id}/actions` `{"action":"触发执行","params":{"时段":"2026-11-23T10:00","触发方式":"定时触发"}}`
  受理后生成采集计划执行回执（时段+执行实例+批次编号），计划进入待确认；
  同一时段并发触发（人工补跑与定时触发同时到达）只生效一次，稍后时段独立受理。
- `{"action":"确认执行"}` 凭回执追查时段与执行实例，计划推进到执行中，
  确认结果进入采集任务待领列表（`GET /api/crawl_job?status=待执行`）。
- `GET /api/scheduler/{id}` 详情返回 `执行回执` 列表；`GET /api/scheduler_receipt?计划编号=...`
  可独立核验回执台账（待确认 → 已确认 → 已认领 → 已写回）。
- `POST /api/crawl_job/{id}/actions` `{"action":"领取任务"}` 按同一份回执取数，
  登记认领结果并回写作业台账；`{"action":"写回台账"}` 完成闭环。
  断线恢复后待领/已认领记录仍可查，重复领取与重复确认自动去重。

领域：数据工程
子方向：数据采集、爬虫与RPA
