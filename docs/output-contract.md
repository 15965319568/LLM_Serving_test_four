# 输出字段

所有 CSV 使用 UTF-8、首行表头、按窗口和主键字典序排序。数值保留可被标准浮点解析的十进制。

`window_summary.csv` 至少包含 `window_id,model,visible_requests,completed_requests,error_rate,p95_queue_ms,p95_ttft_ms,p95_e2e_ms,input_psi,slo_pass,quality_gate,capacity_tokens_s`。

`batch_replay.csv` 至少包含 `window_id,request_id,model,batch_id,visibility,registry_status,queue_ms,ttft_ms,e2e_ms,batch_size,input_token_bin,prompt_hash`，不得包含 `prompt` 或原始 URL。

`batch_reconciliation.csv` 至少包含 `window_id,batch_id,declared_model,observed_models,declared_size,observed_size,completed_size,reconciliation`。

`replica_health.csv` 和 `capacity_replica_detail.csv` 至少包含 `window_id,replica_id,model,node_id,node_status,telemetry_source,authoritative,kv_cache_pct,queue_depth,included,exclude_reason,capacity_tokens_s`。

`drift_findings.csv` 至少包含 `window_id,slice_id,weighted_quality_delta,psi,effective_count,gate`。

`tenant_slo.csv` 至少包含 `window_id,tenant_tier,requests,p95_ttft_ms,error_rate,slo_pass`。

`cost_report.csv` 至少包含 `window_id,model,input_tokens,output_tokens,estimated_cost_usd`。

`routing_plan.csv` 至少包含 `window_id,status,stable_percent,candidate_percent,reason_codes,candidate_capacity_tokens_s,stable_capacity_tokens_s`。

`execution_plan.csv` 至少包含 `window_id,status,owner,target_model,window_start,window_end,action`；`source_reconciliation.csv` 至少包含 `annotation_id,window_id,formal_status,formal_source,reconciliation`。

`execution_plan.csv` 另含 `recovery_status`。`source_reconciliation.csv` 另含 `claim_category,claimed_value,formal_value`。原始备注或认证内容不进入输出。

`control_evidence.csv`：`window_id,event_id,permit_id,disposition`，主键为窗口与事件。

`recovery_options.csv`：`window_id,scenario_id,shard_id,profile_id,selected_revision,eligible,reason_codes,capacity_milli_tokens_s`，主键为前四项，必须覆盖全部组合。没有可见正式 profile 时 selected_revision 留空、容量为 0。

`recovery_assignments.csv`：`window_id,scenario_id,shard_id,profile_id,selected_revision,replica_id,node_id,demand_tokens_s,cost_cents,wave,warmup_start_seconds,cutover_seconds`，主键为前三项；不可行情景没有安置行，但文件仍有表头。

`recovery_decisions.csv`：`window_id,scenario_id,status,total_cost_cents,assigned_shards,required_shards,target_model,static_cost_lower_bound,makespan_seconds`，主键为窗口与情景。status 为 EXECUTABLE/BLOCKED。BLOCKED 的费用为空。相同最优费用的合法方案均可接受。

业务口径见 measurement-contract.md 与 recovery-operations.md。CSV 按本文件所列主键稳定排序；同一输入或仅调换记录顺序的输入必须产生相同输出字节。可以增加解释字段，但不能省略或重复主键。声明的整数按整数输出，延迟误差不超过 0.001 毫秒，其余计算值误差不超过 1e-6。

机器可读的字段、主键和枚举完整列于 `artifact-schema.json`，与本合同同等有效。`registry_status` 必须使用 measurement-contract 中的四个值。批次 reconciliation 只能为 `match` 或 `mismatch`，来源 reconciliation 为 `accepted` 或 `overruled`；布尔 CSV 字段均为小写 `true`/`false`。

recovery_assignments 中 wave 是从 1 连续递增的整数；warmup_start_seconds、cutover_seconds 是相对该窗口 evaluation_time 的整数秒，不是墙上时钟时间。recovery_decisions 的 makespan_seconds 是最后一波的 cutover_seconds；BLOCKED 时为空。static_cost_lower_bound 是忽略波次执行限制但满足选项、共享容量、反亲和及预算的最便宜完整安置费用；不存在时为空，即使最终 BLOCKED 也可能存在该下界。精确执行规则见 recovery-operations.md。

容量明细 telemetry_source 在存在可见权威快照时为 `agent`，否则为 `none`；authoritative 相应为 true/false。排除原因按 no_authoritative_snapshot、node_not_ready、kv_cache_limit、queue_limit 的先后顺序只取第一个，未排除为空；没有节点记录时 node_status 为 `unknown`。batch_reconciliation 的 observed_models 按模型名字典序去重后以分号连接。路由 reason_codes 只取第一个触发门禁的原因，CANARY 时为空。

input_validation.json 必需 files（每个输入的记录数）、window_count、duplicate_request_ids、duplicate_batch_ids。run_manifest.json 必需 input_files（字典序文件名）与 schema_version。observability_contract.json 必需 labels、series_count、max_series、counters、histograms；counters 固定为 requests_total/errors_total/tokens_total（按此顺序），histograms 的 queue_ms/ttft_ms/e2e_ms 均使用 policies.histogram_buckets_ms；series_count 不得超过与 policies.max_metric_series 相等的 max_series。audit.ndjson 每窗口一行，category=decision，含 status、visible_requests、completed_requests、quality_gate；其中完成数包含所有已完成可见请求，区别于排除注册表异常的模型 completed_requests。

可运行 `python scripts/check_contract.py --input fixtures --output output` 检查公开格式与基本交叉一致性。它不验证全部历史业务语义、求解最优性或输入变化，不代表正式验收通过。不得修改合同或该检查工具来绕过失败。
