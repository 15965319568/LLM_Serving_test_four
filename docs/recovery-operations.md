# 发布值班手册：恢复方案的证据与执行边界

路由门禁说明是否希望回滚。执行人员还需要知道，在决策时掌握的证据下，恢复目标是否能真正容纳全部业务分片。本手册适用于 recovery_policy.json 指定的生产 scope；各历史窗口分别决策，不引用当前时间。

## 控制平面的记录

authority_grants.csv 是权限台账。某条 control_events.ndjson 事件只有在台账中存在相同 issuer、operation、scope、model 的授权时才有权改变跨区许可。授权记录必须已被获知（recorded_at 不晚于评估时点），事件的 effective_at 必须处于授权的 [valid_from, valid_to) 内。业务授权和实际操作分开记录，不由岗位名称猜测权限。

事件有两种时间：effective_at 是业务生效时间，recorded_at 是决策系统获知时间。两者都不晚于窗口评估时点才可使用。先检查可见性，再检查 scope，再检查权限。合格事件按 permit_id 重建状态；同一 permit 取 (effective_at, recorded_at, event_id) 最大者。无权事件不能覆盖有权事件；最新合格事件为 revoke 时许可撤销，为 issue 且评估时点达到 expires_at 时许可过期，不复活旧版本。否则许可有效，仅允许指定 shard_id、model 前往 target_region。跨区许可不豁免机房故障、资源、兼容性、预算或隔离要求。

control_evidence.csv 必须逐窗口保留全部事件的处置：not_visible、wrong_scope、unauthorized、superseded、revoked、expired、active。尚不可见的记录只列 ID 与处置，不得拿它的业务值参与决策。

## 发布配置目录

placement_profiles.csv 保留每个 profile_id 的多个发布版本。只在已获知、已生效的 approved 记录中选最大 revision（相同 revision 用 recorded_at 排序）。选出的版本过期时该 profile 不可用，不回退旧版本。draft 不构成发布。没有可见正式版本时也保留该 profile 的候选评估行。

profile 的 model、replica_id 与 region 必须能在 replica/node 库中核实。恢复目标模型来自该窗口的 rollback_windows.csv。tokenizer 必须匹配业务分片，context_limit 至少覆盖 required_context。

恢复吞吐使用现有权威容量：原始 token_capacity_s 乘以剩余 KV 比例，再乘以队列折减比例；队列折减最低为 0.5，分母为 100。节点非 ready、没有可见权威快照、KV 超过策略上限或 queue_depth 大于 12 时容量为零。将未四舍五入的容量乘 1000 向下取整为 milli-token/s，以这个整数做资源约束，不能累计逐项四舍五入后的容量。节点区间与 replica 区间均为左闭右开；节点重叠记录取最大 valid_from，权威快照取最大 sample_time。

## 同一发布的三个恢复情景

对 recovery_policy.json 中的每个情景重新求解。每个分片的需求为基础 demand_tokens_s 乘情景 demand_percent 后向上取整到整数 token/s。unavailable_regions 中的区域不能承接流量。

每个分片整体放在一个 profile，禁止拆分、丢弃、延后计算或少接流量。多个分片可共享同一 replica，但其需求之和不能超过权威容量；同一 anti_affinity_group 的不同分片不能共用 node_id，换 profile 名称不能绕过隔离。同区不需要跨区许可，跨区必须存在本窗口有效且匹配的许可。

最早开始时间是 max(评估时点, window_start)。从评估时点起的等待秒数加 startup_seconds 必须不超过分片 deadline_seconds，完成时间也不能晚于 window_end。不能把已经错过的回滚窗口写成可执行。

cost_cents 是每安置一个分片收取的非负整数分费用；总费用不能超过情景 budget_cents。最终安置还必须满足下文的分批预热与切流协议。在所有完整可执行方案中先最小化总费用，再最小化最后一波切流时间，两个目标都相同的合法方案均可接受。若不存在完整可执行方案，报告 BLOCKED，费用和完成时间留空，assigned_shards=0，不输出该情景的局部安置行；不能挑选违反约束的“最佳努力”方案。

每个窗口、情景、分片、profile_id 都有 recovery_options.csv 行。eligible 只表示这个选项满足本节静态单选项约束，不代表整套方案可行。reason_codes 收集所有失败原因并字典序用分号连接：no_visible_profile、profile_expired、model、placement_region、region_unavailable、compatibility、deadline、capacity、residency。没有正式 profile 时只用 no_visible_profile。capacity 字段保留该 replica 在本窗口的权威容量，即便其他条件不通过。

## 与执行计划的关系

所有窗口都要做恢复预案，包括当前 HOLD/CANARY 的窗口。routing_plan 的状态是发布门禁结论，不能因为恢复困难就改成 CANARY。execution_plan 增加 recovery_status，取 nominal_scenario 对应的可行性；需要 ROLLBACK 且方案可行时 action=rollback，不可行时 action=escalate。HOLD/CANARY 时 action=observe。owner、目标模型和时间窗仍来自正式回滚排期。

方案算法不限，不要求固定步骤或固定的同价选择。所有决策必须能由本次输入重新计算。

## 分批预热与原子切流协议（v3）

本次值班事故还涉及“安置表可行但恢复无法按时上线”。执行器使用 barrier waves：一波内所有分片同时开始加载，等待该波最慢的分片预热完毕后同时切流；上一波全部切流后才能开始下一波。不允许提前提交较快分片、跨波重叠、抢占或重新安置。每个分片恰好参加一波。已有请求持续由原发布服务承接，目标 replica 的吞吐预留一直保留，因此完整安置的共享容量约束仍适用；波次不能复用已预留给先前分片的目标容量。

全部时间以 evaluation_time 为 0，使用整数秒；回滚排期、profile/节点/replica 的有效期边界及控制许可时间均在整数秒边界。workload.release_seconds 是分片允许开始加载的最早偏移，after_shards 是以分号分隔的前置分片 ID，空串表示无前置。所有前置分片必须在更早的波次完成切流。同波不能满足前置依赖。依赖中的 ID 都存在且不重复；依赖环是合法但无法执行的输入，应返回 BLOCKED。

一波最多包含 scenario.max_parallel_loads 个分片，同一 node_id 在同波最多加载一个分片（即使 replica 不同）。profile.warmup_milli_tokens_s 是每个分片加载时的预热资源占用；同波之和不能超过 scenario.warmup_budget_milli_tokens_s。该预算是加载资源，不替代正常 token 吞吐容量；同一 profile 加载多个分片也分别计费和占用。startup_seconds 为正整数，预热资源与预算为非负整数，并发上限为正整数。

wave_start 不早于 max(上一波切流时间、0、window_start 相对评估时点的偏移、波内全部 release_seconds)。wave_end = wave_start + 波内最大的 startup_seconds。同波每一行必须使用相同的 warmup_start_seconds 与 cutover_seconds，后一项就是 wave_end。每个分片的 wave_end 都必须不晚于其 deadline_seconds 与 window_end。

scenario.warmup_blackouts 给出 region、start_seconds、end_seconds。它们是该次决策已知的维护预留，区间为 [start_seconds,end_seconds)，region=* 影响全部区域。只要波内有一个目标区域受影响，整波的 [wave_start,wave_end) 就不能与该维护区间相交。端点相接合法，可以等待维护结束，但不能把预热切成两段。多个维护区间可以相交。

权限和版本选择仍使用评估时点可见的证据，不能利用当时尚未可见的记录改变规划。选定的 profile、replica、node 都必须在实际切流时仍有效：wave_end 严格小于各自 valid_to 相对评估时点的偏移。跨区时至少一个评估时点已 active 的匹配许可必须覆盖切流时点，即 wave_end 严格小于该许可 expires_at 的偏移。多个匹配许可任选能覆盖的一个。这里不把授权台账 valid_to 误当成已发放许可的到期时间，也不预测评估时点以后的新撤销；评估时已知的 expiry、维护预留与排期可以用于未来计划。

recovery_options 的 eligible/reason_codes 保持前文的静态、单选项含义，忽略 release_seconds、after_shards、波次资源、维护和切流时的 lease；因此所有分片各有 eligible 选项，也可能全局 BLOCKED。static_cost_lower_bound 是基于这些静态选项，满足整体吞吐容量、反亲和及预算的完整安置最低费，不考虑波次。实际费用可能高于此下界；不能只对静态最便宜的安置排程，失败就宣布无解，必须考虑较贵但能及时上线的方案。

输出 assignments 的 wave 从 1 连续编号。decisions.makespan_seconds 为最后一波切流偏移，BLOCKED 时为空；static_cost_lower_bound 即使 BLOCKED 也照常保留（静态本身无解才为空）。同价同完成时间的方案都接受，但同一输入和乱序输入必须输出确定的同一方案。execution_plan 的 recovery_status 和 action 必须使用联合安置与排程的结果。

有效输入至少包含一个 workload；workload 的 shard_id 和控制事件的 event_id 各自唯一。after_shards 中的引用均存在，不重复；需求和上下文长度为正整数。profile 同一 profile_id/revision/recorded_at 的记录唯一，不存在无法裁决的并列版本。
