> 历史V7.2验收存档。当前任务以acceptance-v7.3.md为准；本文固定数值和范围不用于本轮评分。

# V7.2 正式验收范围与交付约定

本文是本轮评分范围的完整清单。旧版本文档中范围更广的承诺不自动增加本轮考点。
任务保留整个 serving 工程及杂乱原始材料，但评分集中于以下一条跨模块业务链路。

## P1 数值接纳、实际合批与发布版本

使用公开 Fleet、ServingFabric(workers=2)、ProductionService 的 start/install/infer/close。
公开配置有 north/interactive、south/batch 两个租户/cohort，分别可放置于 east 和 west。
稳定与候选 revision 是配置中的 r20261003、r20261004；CPU MatrixRuntime 执行 value*factor，
本轮采用有限 FP64 输入、offset=0、稳定 factor=2、候选 factor=3。安装全部配置 worker。
合法兼容请求在0.2秒合批窗口内并发到达，每批两个成员，输入 shape=[2,1]，外部 ID 可相同。

发布前使用稳定版本/route_epoch=0；begin 配置候选7500bps且完成 outbox.reconcile 后使用
候选版本/epoch=1。选用的1.0/1.1键按已公开路由分桶均落在候选范围（north为3015/6112，south为1082/2537，阈值7500）。分别对每个版本、每个租户接纳两个请求，共八个正式接纳。同一个原始 key
可以跨租户复用。每个二请求批次共享实际 invocation，响应保留外部 ID、正确数值及接纳 epoch；
worker_pid 属于公开 replicas.pids() 且实际流量覆盖两个不同于控制主进程的 worker。
这些是 CPU serving 语义，不声称测量真实 LLM 精度或 GPU 性能。

## P2 交付完整性与健康判定

使用 ledger.rows、terminal_records、DeliveryInbox.receive/reconcile、assessor.evaluate 等公开接口。
分片 e/w 属于 edge，q 属于 quality；生产者、格式和权威角色由输入配置决定。质量 body 可
复用外部 request_id，正式关联依据仍是 envelope.admission_id。body 的发生时间可以晚于评估窗口。

交付信封按 delivery/1 协议提供 offset、kind、body，事件还有 admission_id。测试倒序交付并重投，
同一合法事实可在不同传输位置重复出现。两个 cohort 各自包含稳定和候选样本，min_samples=2；
edge 与 quality 均须完整，质量下降阈值为0.05。此次不以机器快慢比较延迟，容许足够大的延迟比。

接纳时刻105属于[100,110)，质量 finished_at=300不改变其人群归属。q分片先缺少offset=0，
该位置的同一事实已在另一个位置到达，因此样本本身齐全，仍须因交付缺口判HOLD。
补齐缺口并 reconcile 后，质量全部为1且完整时判PROMOTE。传输重投不得制造额外样本或冲突。

## P3 迟到修订、回滚与持久化重读

健康判定后，对一个候选接纳提交同一质量事件的更高修订(update_seq=2, score=0)，再到达
较旧修订(update_seq=1, score=1)。依据接纳人群和最高合法修订评估，必须判ROLLBACK。
通过 releases.apply 和 outbox.reconcile 实际发布回滚，再正常close并重新打开同一状态目录。

旧的north/key=1.0重读保留历史响应全部字段，包括原worker、invocation、数值、接纳与路由epoch，
无新的接纳。一个全新key使用稳定factor=2、route_epoch=2；累计admitted为9。
内部采用哪些表、文件、字段和schema版本均不属于评分条件。正常close允许重复以完成资源清理。
本轮只检查正常关闭/重开；异常进程退出、断电、旧schema迁移和多阶段曝光不是本轮评分要求。

## D1 修复说明

仓库根目录提交UTF-8 REPAIR_NOTES.md，使用以下四个二级标题，每节填写实际内容：

- `## 诊断证据`：你观察到的请求、原始记录或复现现象及判断依据。
- `## 修复取舍`：改动及选择理由。
- `## 验证`：实际执行过的命令、结果和未验证部分。
- `## 局限`：当前实现或验证的适用边界。

自动验收检查四节存在且非空，不设置字数、关键词答案或实现路线。
文字陈述应与实际工作一致；行为分数仍由真实执行结果决定。

## D2 补充回归测试

在初始工程尚无测试用例的regression_tests/中新增test_*.py，采用Python unittest。
`python -m unittest discover -s regression_tests -v`须发现至少一项用例，全部通过、无跳过。
发现为空、导入失败、失败用例或跳过都不满足交付要求；可以自由选择回归覆盖的相关边界。
自动验收会在独立子进程中实际执行这些测试，不以文件名存在代替执行。该检查不声称证明
作者新增用例覆盖穷尽；P1-P3的业务正确性由验收器自行调用公开API检查。

## 评分与边界

报告、补充回归、联合业务链路三个检查组全部通过为1，否则为0。组数不代表业务断言数。
私有验收逻辑在一个Python文件和一个shell入口内，总计不超过100物理行；JSON输入和模型
manifest/payload只是配置及数据，没有可执行测试、隐藏断言或测试DSL。没有远程加载额外测试。
允许不同内部命名、辅助字段、存储布局和同步实现；不读取内部数据库表或私有对象属性。
历史233项验收不参与本版评分。本版的成功率和轮数必须重新实测，不能沿用旧版成绩。
