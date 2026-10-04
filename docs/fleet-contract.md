# FleetServe 业务与持久化契约（v5.1）

本文规定外部可观察的验收行为。内部模块组织、同步方式和实现算法由维护者决定。
没有要求按文档章节顺序解题。所有时间戳为非负 Unix 秒，可使用注入的时钟。

## 服务与配置

`Fleet(config,state_dir,clock=None,backend_factory=None)`、
`Fleet.from_profile(manifest,state_dir,profile='production',**kwargs)` 提供
`start/close` 异步生命周期，`snapshot()` 提供当前快照。默认后端为实际 MLServer。
可注入符合 StreamServe 后端协议的工厂 `(worker,revision) -> backend`。
同一 state_dir 同时只能有一个控制端所有者；关闭后可重新打开。
进程崩溃恢复的范围是服务进程退出、宿主及存储仍然存活，不要求模拟宿主断电。

profile 的层顺序由 manifest 指定，路径相对 manifest；对象递归覆盖，数组整体替换，
null 删除键。未选择的 profile 和 archives 不参与运行。`load_profile` 返回
`config/digest/provenance/manifest/profile`，provenance 记录生效叶子的最后来源；
digest 为排序、紧凑、UTF-8 JSON 的 SHA-256。配置和 artifact 不能被修成只适配事故样例。
生产凭证与示例凭证无关，仓库只含公开 demo 值。

`artifacts.import_manifest(path)` 校验实际内容的 SHA-256。revision 对应
`tokenizer/context_limit/sha256` 不可变；同值重复导入无副作用，内容不符报
`artifact_digest`，重用 revision 报 `revision_conflict`。

## Worker、放置与接纳

`leases.register(worker,pool,revision,capacity,ttl=60)` 每次注册递增该 worker 的持久化
generation，新代次初始 sequence=0、free=capacity、state=ready。容量不超过 pool。
`heartbeat(worker,generation,sequence,free,ttl=60)` 仅接受当前 generation 的更大 sequence，
返回 `accepted/generation/sequence`；旧心跳不续租、不改容量，也不能取消 drain。
`drain(worker,generation)` 的代次不符报 `worker_generation`。

允许放置需要同时满足：租约 expires 严格大于当前时间、ready、目标 revision、tokenizer、
候选 pool、租户允许区域，以及剩余容量。free 是外部容量快照，还须扣除本控制端绑定到
同一 worker/generation 的 admitting、queued、running 请求。相同可用条件下按
reservations、worker 名排序。无可用目标报 `no_healthy_capacity`，不能悄悄改走另一个 revision。

路由 plan 包含 `stable/candidate/candidate_bps/salt/pools`。
candidate_bps 在 1..9999；分桶为 SHA256(`salt + NUL + tenant + NUL + key`)
前8字节按大端转整数模10000，小于 candidate_bps 走候选。新发布 salt=release_id。
每个 alias 的 epoch 独立，从0开始。

`gateway.submit(Request)` 返回全局 ticket，包含
`id/tenant/alias/revision/pool/worker/generation/route_epoch/state/completion_tokens/terminal_reason`。
首次接纳固定实际 revision、worker/generation、epoch 和底层 journal；相同 tenant/key
及原始请求内容必须返回原身份，包括跨发布、替换 worker、重启和 drain 后重放。
同键不同内容报 `idempotency_conflict`；不同租户的同键独立。完成、取消和失败均为终态，
只能记账一次；重放不再计费、不重新生成。不可将当前 alias 指向当作历史请求的实际 revision。

`gateway.stream(tenant,id,after=0)` 返回全局 ID 的 StreamServe 事件；accepted 增加
pool、worker、generation、route_epoch。断线只关闭订阅，保留请求；合法游标只重放后缀。
`status/cancel` 只能访问所属租户；其他租户得到 `not_found`。`drain()` 停止新接纳，
等待现有请求；`close()` 释放 worker、子进程和所有权。重启不自动重算未完成推理；
缺失底层接纳记录记为 admission_interrupted，已有 journal 按持久化契约恢复。

## 发布与边缘路由

`releases.begin(release_id,alias,revision,candidate_bps,expected_epoch,operation_id)`
需要独立候选版本、无当前 canary、所有租户均有 stable/candidate 合法容量。
以 expected_epoch 比较，失败报 `epoch_conflict`；校验失败不能提交部分决策。
发布保存开始时的 policy。修改当前配置不得改变既有发布的判定阈值。
operation_id 跨 begin/apply/rollback 全局唯一；同操作同参数重试返回原结果，
更换参数或操作种类报 `operation_conflict`，不重复递增 epoch 或 audit。

控制库提交产生 canary_pending；`outbox.reconcile(limit=100,after_apply=None)`
将独立边缘库推进至 canary。边缘提交成功但控制端未收到 ACK 时可以重送。
同 effect 身份及内容幂等；内容不同报 `effect_conflict`。首次 epoch=0，此后连续；
跳跃报 `edge_epoch_gap`。未知的旧 effect 返回 stale，不能把边缘路由退回旧值。
after_apply 是模拟 ACK 丢失的边界，可抛错或退出进程；启动应继续 pending effects。
仅当前发布 epoch 的 ACK 可更新阶段。

`assessor.evaluate(release_id,start,end,assessment_id)` 仅对 canary 阶段形成不可变快照，
结果含 assessment_id、release_id、route_epoch、evidence_version、window、decision、
reasons、metrics、mix_distance、evidence_digest、sample_ids、ignored。
同 assessment_id 同窗口同证据同epoch可重读；更换快照报 `assessment_conflict`。

以下为未配置policy.rollout的原有语义；启用后补充遵守progressive-rollout-contract。
`releases.apply(assessment_id,operation_id)` 必须同时验证 canary、route_epoch 及
evidence_version；任一变化报 `stale_assessment`。HOLD 不改路由和epoch；
PROMOTE/ROLLBACK 重新核验目标容量，成功提交 epoch+1、terminal_pending，
ACK 后分别 promoted/rolled_back；失败不留下部分状态。
`rollback(release_id,expected_epoch,operation_id)` 为人工回退，同样遵守幂等、CAS及容量。
`controller.tick(start,end)` 对在观察发布评估、应用、同步边缘，重复调用可恢复，
终态发布不重复评估。重启须使用持久化路由，不能用 bootstrap 配置覆盖已发布状态。

## 管理与兼容

control.sqlite3 的 user_version=1和2均已存在；升级到3只能加法迁移，保留租约、路由、操作、
接纳、证据和审计。未知更高版本报 `unsupported_schema`。V1 表定义见 migrations.V1，
V2 为 receipts 增加 source='gateway' 及查询索引；V3增加逐阶段发布状态与窗口历史，见progressive-rollout-contract。edge.sqlite3 与 worker journals 独立。
底层 StreamServe 公开契约与19项原有兼容测试继续有效。

`operations.reconciliation.accounting(store)` 按 tenant/alias/revision 汇总 admitted、
completed、failed、cancelled、active、tokens，满足请求数守恒且重启一致。
`route_consistency(fleet)` 比较真实控制/边缘状态；audit_page(after,limit) 为严格递增游标，
after>=0，1<=limit<=1000。metrics 禁止 request_id、prompt、key、凭证等无限标签。

HTTP 鉴权采用 Bearer；tenant 来自凭证。POST `/v1/generate` 用 Idempotency-Key，
body 为 model、prompt、max_tokens、stop、stream；非流式返回202。GET/DELETE
`/v1/requests/{id}` 及 GET `.../events`（Last-Event-ID）保持这些语义。
管理员接口 `/admin/releases/evaluate/apply/rollback/reconcile/workers/heartbeat/worker-drain/
telemetry/receipts/watermark/drain` 为 POST，对应同名 Python 接口的 JSON 参数。
GET `/admin/state/accounting/consistency/audit` 和 `/metrics` 均需管理员认证；
`/health` 公开。无权限调用必须先拒绝且无副作用；不依赖客户端提供的租户名称。
