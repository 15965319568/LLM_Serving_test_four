# V7.3 正式验收范围与公开业务契约

本文是本轮评分义务的完整清单。保留整个成熟工程，是为了诊断真实调用链与历史兼容路径。
历史契约提供接口参考；只有本文选取的行为参与评分，不要求修复所有历史功能。

## P1 接纳事实、真实合批与在线发布

入口是 Fleet.from_profile、ServingFabric(workers=2) 和 ProductionService 的 start、install、
submit、wait、cancel、infer、terminal_records、accounting、close，以及 ledger.get。
Fleet 的 leases.register/get、releases.begin/apply、outbox.reconcile 和 assessor.evaluate
沿用公开语义。使用 north/interactive 与 south/batch 两租户，区域 east/west，各有稳定和候选
revision。配置声明 worker、区域兼容关系、模型 manifest、策略及 producer 注册信息。

同一 tenant/key 的历史请求身份独立于当前路由。不同租户可以复用同一个 key；客户提供的
request.id 可以重复。接纳时绑定的版本及 route_epoch 不随之后的安装、发布或回滚变化。
业务发布经 outbox.reconcile 生效；install 的部署 epoch 与业务路由 epoch 是两种版本。

本轮使用 MatrixRuntime 的 FP64 数值载荷：单输入 x，shape=[n,1]，n 在1至3之间，有限且可为
负数的输入、factor 和 offset；结果逐元素为 value*factor+offset。一个批次内参数一致，
不同租户的 offset 可以不同，行数可以不同。响应保留外部 ID、各请求自己的数据和接纳 epoch。
合法兼容请求在配置的批窗口内一同接纳，窗口足以容纳所有成员；每组填满 max_batch_size，
配置大小不超过3。必须形成一次实际物理调用，而不是分别计算后伪造共同 invocation。
合批按请求成员计数，物理 tensor 的行数为成员行数之和。两个真实 worker 均应承担请求。

在线安装同 revision 的新部署以及候选路由发布可与已开始的旧批次重叠。旧请求继续按原部署
执行，新接纳按已生效部署/路由执行。在旧预测被明确屏障阻塞时，其他部署安装和候选请求
应能继续完成，不能把整个系统暂停到旧请求结束。显式 cancel 后账本终态为 cancelled；
同批未取消成员仍得到正确结果，晚到的物理响应不能把取消改成完成。整批调用方均取消时，
物理计算可能仍在运行；旧实例必须在物理执行结束后才能卸载。正常 close 可重复并释放资源。

### 公开运行观察点

install 的 runtime_options 支持现有 MatrixRuntime 的 events（JSONL目录）和 predict_gate
（文件路径）。predict 在开始后等待 gate 文件存在；这是确定故障时刻的演练屏障，不是性能
指标。events 每个 worker 一个文件，记录 kind、pid、version；started/finished 还记录真实
invocation 和 rows，unloaded 记录 active。事件须如实反映实际执行，不能为验收伪造。
ServingFabric.replicas.pids() 给出实际 worker PID。验收通过这些公开事件与响应关联检查
真实合批、双 worker 覆盖和卸载时 active=0，不检查私有引用计数、锁或缓存结构。
使用合理的等待保护避免错误实现挂死，不按机器吞吐量或推理耗时排序评分。

## P2 有来源的证据、交付完整性与质量判断

使用 DeliveryInbox(fleet, partitions)、receive、reconcile、close。partitions 是配置 producer
到运输分片的登记关系；同一个 producer 可以有多个分片。输入为 delivery/1 信封，offset
从0开始，kind=event 时有 admission_id 和 body，kind=watermark 时 body 有 cohort、through。
质量 body 中的客户 ID 可重复；正式关联依据是 envelope.admission_id。

保留以下生产数据语义，字段单位及格式参考 docs/evidence-contract.md 与 production-contract.md：

- event-v2 与 export-v1 都会使用。export-v1 缺少可选 version 时合法地取0；其 finished_ms
  是毫秒，event-v2 的 finished_at 是秒。不能粗暴丢弃所有旧格式或缺可选字段的记录。
- producer 的 format、role、rank 来自配置登记。gateway 提供真实终态；quality 提供质量；
  advisory 只能提供观察。同一接纳的质量选择最高登记权威，其内部依合法修订处理。
  低权威 producer 即使自称高 rank、携带更晚时间或更大 update_seq，也不能越权覆盖。
- 同一事件的较高 update_seq 优先于较低修订。到达顺序、运输 offset、发生时间不能代替
  事实修订。合法 score 在[0,1]；越界事件独立隔离，不能阻断合法邻居或污染质量。
- 同一位置的相同信封允许重投；同一事实也可出现在不同位置。两种重复都不能制造额外
  业务样本。合法交付可能正序、倒序或重复；允许重排的是遥测投递，不是接纳/发布顺序。
- 必需 producer 的每个分片均须连续处理到对应 watermark，才能形成完整性声明；任一
  分片的缺口即使没有导致样本缺失，也仍然不完整。不能把最大到达 offset 当作连续进度。
  不合法 body 的已隔离记录算已处理；运输中根本未到达的位置不算。水位对 producer 的
  所有登记分片取共同可确认的边界。不同 producer 和 cohort 的进度不能混用。

评估使用接纳时刻属于[start,end)的全部正式接纳，包含后来取消的请求；质量回填晚于窗口
不改变样本归属。assessment.sample_ids 是这一人群的排序身份列表，不能按当前路由或
外部重复 ID 合并、丢弃。完整性不足时保持 HOLD；完整健康的人群可 PROMOTE；候选 cohort
的质量相对稳定基线退化超过配置阈值时 ROLLBACK。按配置 min_samples、required_cohorts、
required_sources、max_quality_drop、max_error_rate 等评估，不能固定返回一种决策。
本轮延迟比与 mix 阈值设为宽容值，避免用机器速度决定分数；不新增算法或阈值规则。

## P3 采集恢复、回滚与持久重读

DeliveryInbox.reconcile(after_apply=callback) 的公开观察点位于单条交付业务效果已生效、
运输收件箱确认之前。可在 callback 中直接退出当前采集子进程，随后用同一持久状态重新
打开服务和收件箱并 reconcile。该重放不得丢失已经生效的事实，也不得重复增加样本。
观察点必须按上述含义执行；不能跳过 callback 或先确认再执行业务来规避恢复情形。
故障作用于采集进程，发生在推理请求已有终态后；不要求活跃物理推理跨任意崩溃恰好一次。

同一人群后续收到权威质量高修订与更晚到达的低修订，应按有效事实重新判断。回滚经过
releases.apply、outbox.reconcile 后实际影响后续新请求；如果稳定 revision 在此前已安装
了新部署，新请求用当前稳定部署的 factor 和新路由 epoch，不能复活被替换的旧部署。
已完成 tenant/key 重读必须返回持久化的完整历史响应，包括原 PID、invocation 和数值；
重启或路由改变不会再次执行或新增接纳。累计 admitted 等于唯一已接纳的 tenant/key 数量，
包括取消的请求。取消的旧请求在采集恢复后仍保持取消，不被质量记录或晚响应改写。

## D1 修复说明与 D2 补充回归

根目录提交UTF-8 REPAIR_NOTES.md，包含四个非空二级章节：

- `## 诊断证据`：实际观察与诊断依据。
- `## 修复取舍`：改动及其理由。
- `## 验证`：运行过的命令、结果与未验证部分。
- `## 局限`：实现或验证的适用边界。

自动验收检查标题和非空内容，不设置字数、固定答案或修复路径；不自动评判文字真实性。
在初始没有用例的 regression_tests/ 中新增 test_*.py，使用 Python unittest。
`python -m unittest discover -s regression_tests -v` 必须发现至少1项，全部通过且无跳过。
验收会在独立子进程中实际执行。你可以选择与修复有关的回归，不规定具体文件名或用例。
这项检查证明新增回归可执行，不声称证明其覆盖穷尽；业务链路由验收器独立检查。

## 评分和自由边界

两组检查为交付物、联合生产链路；全部通过为1，否则0。生产链路使用同一公开规则下的
多组数值、取消组合和交付排列。所有控制、断言和结果判断在一个 Python 文件与一个 shell
入口内，合计不超过100物理行；JSON仅包含配置、请求、原始交付与独立预期数据，无测试DSL、
远程验收包或隐藏helper。断言按生产前后关系执行，前置失败时相应子场景不会继续。

允许内部表名、文件名、schema版本、辅助字段和同步策略不同，不查询数据库表或私有属性。
本版检查上述有限 CPU 生产链路，不检查完整 HTTP/SSE、任意进程故障、所有旧profile、
旧库迁移、输出选择、容量竞争等历史细项。不能把历史文档中的广泛承诺当作隐藏考点。
报告与测试以实际交付核对，不要求排查步骤或调用次数。模型成功率与轮数须另做本版五测。
