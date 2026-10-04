# 区域数值服务与原始采集契约

本契约补充 fleet、evidence、progressive-rollout、fabric 与 MLServer 契约。旧业务也是待修范围。
同一组生产请求必须实际经过区域放置、MLServer 数值执行和证据评估，不能以静态事故报告代替。
本文定义外部行为，不规定表结构、内部辅助函数、同步策略或排查顺序。

## 接纳与执行

`ProductionService(fleet,fabric)` 组合既有 Fleet 和 ServingFabric，`start/close` 管理两者生命周期。
`install(worker,generation,factor,operation_id,expected_epoch=0,**options)` 将当前租约对应的数值规格
装载到实际双副本。generation 不等于当前租约报 worker_generation。options 为 Fabric 的批大小、
批窗口与 CPU 故障演练参数。install 的部署 epoch 与业务路由 epoch 是不同概念；安装模型不等于
发布客户路由。实际执行实例身份不能因同名 worker 被重新注册而混用。

`submit(tenant,alias,key,InferenceRequest)` 返回已接纳的 ticket，`wait(tenant,id)` 返回数值响应；
`infer` 为二者组合。ticket 至少包含 id、tenant、key、external_id、alias、revision、route_epoch、
worker、worker_generation、pool、cohort、admitted、rows、state、response、error、finished、latency_ms。
内部执行 alias 可额外提供，但不规定命名算法。响应包含真实 worker_pid、invocation、receipt_id、
deployment epoch（Fabric 原字段 epoch），并增加 admission_id、route_epoch、worker、worker_generation。
所有身份应当能够关联，不能伪造 PID 或在主进程计算来模拟 worker。

路由按边缘已发布快照执行；仅控制库中的 pending 变更尚不生效。接纳时选定的租户区域、实际
revision、worker 代次、路由 epoch 和 cohort 为历史事实。之后发布、回滚或改配置不能更改历史。
继续遵守已有租约、tokenizer、区域、过期、drain 和容量约束；本入口活动请求也占一份本地保留
容量，与流式入口的保留容量合计。容量不足不能换到另一个 revision。

同一 tenant/key 与相同原始 alias/完整请求内容返回原身份和终态结果，包括跨路由更新、租约
替换、停止接纳及重启；相同 key 不同原始内容报 idempotency_conflict。不同租户相同 key 独立。
外部 request.id 允许重复，不能作为接纳或重投去重主键。无效 tensor、未知租户或租户冒用不得
创建接纳或底层计算。公开业务 alias 无对应已安装数值执行实例时可以报 unknown_alias。

`ledger.rows(tenant=None)`、`ledger.get(tenant,id)` 提供可审计接纳记录；其他租户访问报 not_found。
completed/cancelled/failed 为不可逆终态。`cancel(tenant,id)` 幂等；不能因旧响应晚到转成 completed。
生产入口的等待者离开只解除等待，工作继续，明确 cancel 才取消请求。此规则属于新生产入口；
旧独立 Fabric HTTP 的断开即取消规则继续有效，两种客户端有不同约定。

进程崩溃后已持久化成功响应可原样重读；没有持久化终态的接纳恢复为 failed/restart_interrupted，
不自动重算。实际子进程可能已执行但结果尚未持久化，不能据此发明成功响应。close 取消本入口
尚未完成的逻辑请求并等待物理资源安全释放。主机及存储仍存活，不要求主机断电恢复。

`accounting()` 按 tenant/alias/revision 汇总 admitted、active、completed、cancelled、failed、
completed_rows。请求数守恒，重投和重启不能新增账单。返回的响应对象允许调用者修改，不能污染
以后重读的持久化响应。

`sync_evidence()` 将接纳身份同步到 Fleet.evidence，同值幂等，绑定业务 alias 和接纳事实。
`terminal_records(tenant=None)` 返回可供采集的 event-v2 终态记录，event_id 对该接纳稳定、
update_seq=0、数值单位 ms、revision 为本次实际执行版本。该接口不直接推进遥测完整性水位。
质量是独立回填，不得在缺少质量事实时自行补零或补健康。

## 采集分片与原始文件

`DeliveryInbox(fleet,partitions,path=None)`，partitions 是 `{分片名: 已配置生产者名}`。该注册表
对同一持久化收件箱不可改变，否则 partition_registry_conflict。不同原始格式、role 与 rank
继续来自生效的 producer 配置；消息自报 producer/rank 不具有权限。

`receive(partition,records)` 接收最多 10000 项，返回 inserted、duplicates、rejected(index/code)。
记录信封：schema=`delivery/1`、offset（从 0 开始的非负整数）、kind、body。kind=event 时还需
admission_id；body 是所属生产者协议的原始记录。kind=watermark 时 body 为 cohort、through。
文件交付可以乱序，offset 是该分片中的原始位置。不同分片 offset 可以相同。

相同分片/offset/完整信封重投为 duplicates，不重新摄取；相同位置不同内容报 delivery_conflict，
已接受内容保持不变。这代表运输协议违反位置不可变约定，不要求在非法矛盾流的不同首次接受
顺序之间选出相同真值。业务事实的修订或撤销必须使用新 offset，body 中的事件身份和 update_seq
继续按证据契约解释，不能把新的传输位置当作更高事实修订。

`reconcile(after_apply=None)` 尝试交付 pending 消息。event 使用 admission_id 查找正式接纳事实，
并绑定为 producer 记录中的 request_id（export-v1 为 request）；body 中重复的客户端 ID 不作为
关联依据。没有正式接纳身份时保留 pending，重开收件箱后仍可再关联。原始 body 保留不改。
一条合法信封内的非法事件被独立隔离，保留分片/offset/原因，邻居可继续交付。缺失可选字段
仍可合法；不能把全部旧格式、缺字段记录或所有重复都丢弃。

watermark 只有在本分片此前所有 offset 已被处理（成功交付或明确隔离）后才能通过；缺口和
仍待关联的事件都会阻止这条声明。生产者对某 cohort 的可见完整性由该生产者所有已配置分片
共同确认，以其中最低的通过时间为界；任一分片尚无声明就不能宣称该生产者已完整。水位单调。
不能把最大已到达 offset 当成连续已处理进度。after_apply 是交付已生效、收件箱记账前的崩溃
观察点，可抛出 RuntimeError 或退出进程；恢复重投不能重复改变有效事实。

`status()` 返回各 partition/producer/next_offset/pending/rejected；next_offset 是从 0 开始尚未
连续处理的首个位置。`audit()` 返回保留的 partition、offset、state、reason、payload，用于审计。
payload 可用 JSON 字符串或等价对象表示，state 使用 pending/applied/rejected。close 释放持久化
所有权，再打开后保留已交付、待关联、隔离和水位信息。

`import_manifest(path)` 读取 UTF-8（允许 BOM）manifest，files 项为 partition、path 和 format
（jsonl 或 csv，默认 jsonl）。路径相对 manifest；不能越出该目录。JSONL 可 gzip 压缩，空行忽略，
坏 JSON 行按原始 line 独立返回 invalid_json。CSV 可带 BOM，列为 schema/offset/kind/admission_id/body，
body 是 JSON 对象文本。函数返回逐文件 inserted/duplicates/rejected(line/code)，并尝试 reconcile。
原始文件顺序不等于事件发生顺序。坏 JSON 无法可信地确定其 offset，不得凭猜测填补分片缺口。

同一合法事实集只改变分片文件顺序、重复交付或摄取批大小，最终有效事实和业务判定应一致。
不要求中间 inserted/older 计数或 evidence_version 在不同历史中相同；晚到的新事实也不得改写
已经保存的旧评估快照。原始证据、标准化事实、评估人群、路由状态是不同的可观察对象。

## 联合生产接口与复现

`ProductionApplication(service,inbox)` 保留原 Fleet HTTP 管理和流式接口，并提供：

- POST `/v2/models/{business_alias}/infer`：V2 请求体，Bearer 决定租户，Idempotency-Key 必填。
- GET `/v2/receipts`、GET/DELETE `/v2/requests/{id}`：所属租户的记录与取消。
- POST `/admin/numeric/install`：install 参数；GET `/admin/numeric`：accounting 与真实 workers。
- POST `/admin/capture/receive`：partition/records；POST `/admin/capture/reconcile`；GET `/admin/capture`。

管理鉴权须在副作用之前完成。客户端断开不撤销已接纳的生产请求；重连使用原 key 取得原结果。
既有 `/admin/releases/evaluate/apply/reconcile` 等接口仍使用同一 Fleet 对象。阶段推进、晚修订
重核、ACK 重投和重启后的发布继续遵守原灰度契约，并实际影响后续数值请求。

运行方式见 README。`python -m fleetserve.production.replay --output 新目录` 会真实运行合成 CPU
演练、保存多来源原始记录及采集副本。这是观测工具，不输出标准答案，不保证起始版满足契约。
