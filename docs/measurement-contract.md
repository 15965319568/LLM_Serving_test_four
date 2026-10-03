# 生产观测口径

所有聚合均是从导出开始到评估时点的累计观测，按各窗口独立截断，不是相邻窗口之差。到达时间不晚于评估时点的请求进入回放；完成时间也不晚于评估时点才进入完成指标。回放保留全部可见请求，包括注册表异常；这些异常不进入模型完成指标、租户指标或成本。visible_requests 包含这些异常，completed_requests 只计注册表合格记录。

注册表按 unknown_model、tokenizer_mismatch、context_limit 的顺序裁决；均不命中为 valid。回放中的 queue/ttft/e2e 从 arrival_time 分别到 dispatch_time/first_token_time/completed_time，单位毫秒；未完成请求这三项留空。prompt_hash 为原始 prompt 的 UTF-8 SHA-256 前 16 个十六进制字符，不保存原文。token 分桶为左闭右开，落在范围外时使用末桶。

延迟百分位使用策略中的 nearest_rank，空集合取零且 SLO 不通过；status 不为 ok 的完成请求计为错误，错误率分母包含全部合格完成请求。模型 SLO 同时满足三项延迟与错误率的上界才通过。租户指标合并稳定及候选模型，按租户等级判断。成本由合格完成请求的输入、输出 token 分别乘对应每千 token 费率计算。

质量评估是独立的离线抽样数据，不从服务请求数推算。每个 slice 优先使用本窗口的全部桶，没有该窗口记录时使用该 slice 最近的可见窗口全部桶。窗口先后以 policies 的 evaluation_time 确定，不以文件顺序或 window_id 字典序确定。每桶的统计质量为 weight × effective_count，weighted_quality_delta 是以该质量加权并归一化的 candidate_score-baseline_score，不能把桶差直接求和。有效样本为 effective_count 之和。PSI 将对应桶的原始 baseline_count 和 candidate_count 各自归一化，非正计数用 1e-9；请求输入 PSI 也采用同一口径。

每个 slice 先判断有效样本不足，再判断质量差或 PSI 达到失败界，最后判断 PSI 警告界。边界包含在相应触发条件内。该版本的整体质量 gate 取 fail 优先于 warn，否则 pass；insufficient 在切片明细中保留，不单独触发自动回滚。

路由使用正式 approval_records（不使用部署配置中的审批缓存）。审批 scope/model 必须匹配，signed_at 可包含评估时点，revoked_at 在评估时点即失效。候选启动时间及该窗口 feature flag 都满足才可放行。事故的区间为 [event_time, end_time)，空 end_time 表示尚未关闭，model=* 对候选同样有效。门禁依次为：审批或开关不满足→HOLD/approval_or_feature_gate；回滚事故→ROLLBACK/active_incident；质量 fail→ROLLBACK/quality_drift；gold 租户或候选 SLO 失败→ROLLBACK/candidate_slo；hold 事故→HOLD/active_incident_hold；候选容量不足→HOLD/capacity_below_target；否则 CANARY。CANARY 候选比例取正式窗口配额与 max_canary_percent 的较小值，其他状态取零；稳定比例是剩余部分。这是期望路由，执行可行性另见恢复手册。

批次只在 event_time 可见后对账，关联可见请求；声明大小与模型集合均一致时为 match，否则为 mismatch，不能让错误声明覆盖观测数。副本容量口径见 recovery-operations.md；replica_health 与 capacity_replica_detail 必须一致。

运维备注按 event_time 归属第一个不早于该时间的评估窗口。claim_field 分别在正式路由比例、整体质量 gate 或 node_id 的有效状态中取值，与 claimed_value 比较，相等为 accepted，否则 overruled。正式来源分别标为 routing_plan、drift_findings、node_inventory。路由历史在它声明的 window_id 内且 event_time 已可见时对账，annotation_id 为 history:<window_id>:<event_time>，claim_category=route_history。备注、历史、审批缓存都不能改变正式计算。

JSON 产物 input_validation.files 必须列出全部输入，CSV/NDJSON 计数据行，JSON 对象计 1；重复请求/批次 ID 计原始行数减唯一 ID 数。run_manifest 记录所有输入文件名和 schema_version。审计逐窗口记录真实状态、可见数、完成数、质量 gate，不写原始身份或 payload。观测 labels、counter、histogram 桶遵循 policies，series_count 不得超过 max_metric_series，禁止身份或 payload 作为标签。产物不包含机器绝对路径。
