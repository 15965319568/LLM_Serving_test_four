# 遥测与判定契约（v5）

事故包是本地合成演练的原始导出，保留重复、乱序、旧协议、修订、截断行及建议性注释。
`operator-notes.md` 为当班人员的未证实假设；正式判定依据是生效配置及本文。
不能直接相信看板总平均，也不能把所有旧记录一律丢弃。

`EvidenceStore.receipt` 接收 request_id、tenant、alias、revision、cohort、admitted、
kind(live/shadow/replay)、source（默认gateway）；只有gateway接纳身份有效，身份不可改写。
同值重复无副作用，冲突报 receipt_conflict；tenant/alias 必须存在。

`ingest(producer,records)` 根据配置的 producer 协议、role、rank 解释记录。
records最多10000项，独立拒绝非法行，不能让一个坏行吞掉邻居；返回 inserted、updated、
duplicates、older、conflicts、rejected(index/code)、evidence_version。

| 协议 | 字段与单位 |
| --- | --- |
| event-v2 | schema=2, event_id, update_seq, request_id, finished_at(秒); latency及latency_unit(ms/s/us); outcome(completed/failed/cancelled); score(0..1) |
| export-v1 | schema=1, id, version(默认0), request, finished_ms; duration_ms; status(ok/error/cancel); quality(0..1) |

两种格式可含 revision 声明、sample_kind（默认live）、deleted（布尔，默认false）。
gateway role 必须有合法终态和非负时延；quality role 必须有合法质量值；deleted
不要求被撤销的数值。生产者身份、rank和role来自配置，不能由payload自报或到达次序决定。
秒转毫秒乘1000，微秒转毫秒除1000；所有数值有限，拒绝NaN/Infinity。

事件身份是 producer/event_id；只取最大修订号。相同修订相同归一化内容为重复；
同修订不同内容为未决冲突，直到更高修订修复。tombstone保留修订，不允许旧记录复活。
原始保留状态通过 export_raw 可审计。有效内容变化、首次冲突、receipt或水位推进改变
evidence_version；完全重复、较旧修订、非法输入和不前进水位均不改变。

`watermark(producer,cohort,through)` 单调声明该 cohort 中接纳时间小于through的事实完整，
不是网络到达时间水位。评估人群按接纳时间 `[start,end)`，quality 可以晚到。
只有实际接纳到评估 alias、stable/candidate revision、live 且窗口内的请求计入。
排除未知接纳、shadow、replay、非目标revision、revision声明不符及advisory来源。
绑定依据是原始receipt，不是当前alias配置或遥测自报cohort。

同 request_id/role 优先最高rank；同最高rank不同事实为冲突。相同事实任选确定性的
producer/event_id次序，不能重复计数。合法窗口内的权威未决修订冲突使该cohort不完整。
每个有gateway终态的请求形成一条性能样本，可关联一条quality样本；缺quality不补零。
输出保留样本来源身份及ignored原因，排序保持确定性，digest包含样本、排除和冲突信息。

按发布冻结policy的 required_cohorts 分别比较stable/candidate。每组须同时满足
两侧性能样本数与quality样本数均>=min_samples、required_sources水位均>=end、无权威冲突。
不完整人群不能提供可靠退化结论。完整人群的以下任一条件为退化：候选错误率
（非completed个数/全部gateway终态数）>max_error_rate；候选p95>
stable_p95*max_latency_ratio；stable_quality_mean-candidate_quality_mean>
max_quality_drop（浮点比较允许1e-12容差）。p95为nearest rank，即排序后ceil(.95*n)项。
quality均值只用实际关联的质量样本。

有完整cohort退化则 ROLLBACK（即使其他cohort缺数据）；否则任意cohort不完整则 HOLD；
否则两侧流量mix的 total variation `0.5*sum(abs(p_stable-p_candidate))`
大于max_mix_distance则 HOLD；其余 PROMOTE。阈值相等不视为超过。
reasons使用 `cohort:error_rate/latency/quality`、`incomplete:cohort` 或 `traffic_mix`，
同一类别多项排序，不要求说明文档固定措辞。

导入工具支持带BOM CSV、JSONL、gzip。非法JSON行独立记录line/code，空行忽略；bundle的
receipts、sources、watermarks均由相对路径或值引用。重复导入幂等。
`compat.dashboard` 继续提供 export-v1 全量离线摘要；`compat.route_preview` 解释旧百分比
schema。它们有历史用户，不是正式发布决策接口，也不能删除来回避冲突证据。
