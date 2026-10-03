# 连续批处理推理服务观测合同

## 输入

`fixtures/` 是 synthetic 数据，包含 22 份来自不同系统的原始导出：

- `policies.json`：窗口、模型、延迟/错误 SLO、漂移阈值、分桶、批处理和观测标签策略。
- `request_events.ndjson`：请求到达、派发、首 token、完成时间，模型、租户、token 数、批次和结果。
- `batch_events.ndjson`：批次形成和调度事件，用于核对批大小与队列延迟。
- `replica_inventory.csv`：replica 的模型、节点、有效区间和 token 吞吐能力。
- `runtime_snapshots.csv`：权威 agent 与 sidecar 的 GPU/队列/KV 快照。
- `quality_evaluations.csv`：按窗口、切片和分桶的 baseline/candidate 质量与有效样本。
- `tenant_slo.csv`：租户等级 SLO。
- `deployment_policy.json`：模型上线、路由比例和审批有效期。
- `model_registry.json`：模型、tokenizer、上下文上限和稳定/候选角色。
- `node_inventory.csv`：节点区域、状态和有效区间。
- `cost_rates.csv`：输入/输出 token 成本口径。
- `approval_records.ndjson`：带 scope、生效、撤销时间的正式审批。
- `feature_flags.json`：按窗口生效的候选开关。
- `incident_events.ndjson`：窗口内事故及其影响模型。
- `rollback_windows.csv`：每个窗口的负责人、目标版本和执行时间窗。
- `route_history.csv`：历史路由记录，只用于对账。
- `operator_annotations.ndjson`：非权威的运维备注，只能做对账，不能改变策略。
- `authority_grants.csv`：生产控制平面的权限台账。
- `control_events.ndjson`：跨区域恢复许可的发放、撤销与获知记录。
- `placement_profiles.csv`：恢复配置的发布历史、预热时长与预热资源占用。
- `recovery_workloads.csv`：必须恢复的业务分片及服务承诺、释放时点和上线前置依赖。
- `recovery_policy.json`：同一次发布需要评估的恢复情景、安置费用预算、预热并发预算和维护区间。

所有时间是带 `Z` 的 UTC ISO-8601 字符串。窗口可见性使用 `event_time <= evaluation_time`；请求只有在完成时间不晚于评估时点时才进入延迟聚合，仍在途请求要在回放中标记 `in_flight`。

## 计算与判断

1. 按正式注册表校验请求使用的模型和 tokenizer；异常记录要能在逐请求明细中追溯，并且不能悄悄混入质量或成本汇总。
2. 对每个窗口和模型独立聚合请求数、错误率、输入分布、排队、首 token 和端到端延迟。百分位口径从正式策略读取，不能用平均数替代。
3. 根据批次导出与请求事件恢复批次大小、模型一致性、已完成数和 token 负载，输出批次对账；同一批次不能跨窗口拼接。
4. 结合 replica 有效区间、节点状态、权威 runtime snapshot、KV/队列阈值和模型注册信息重算容量。不同采集源发生冲突时保留冲突证据，但容量只能来自正式有效数据。
5. 对可见质量评测按切片权重和对齐分桶计算漂移；有效样本不足、质量变化或输入分布变化是否阻断，要从策略和原始评测共同判断。
6. 结合审批 scope、生效/撤销时间、feature flag、事故、租户 SLO、性能、容量和正式路由策略给出 `CANARY`、`HOLD` 或 `ROLLBACK`，并把决策原因映射到回滚负责人和时间窗。
7. 从成本费率和逐请求 token 汇总成本；保留历史路由与运维备注的对账结果，但不能让它们覆盖正式来源。所有窗口、明细、汇总、成本和执行计划必须相互可复核，输出排序稳定，不能使用当前时间或绝对路径。

## 输出合同

详细的生产口径、控制平面规则、字段与边界分别在 `docs/measurement-contract.md`、`docs/recovery-operations.md` 和 `docs/output-contract.md`。实现需要支持这些 schema 内的其他有效输入，不能缓存本次夹具的答案。

- `window_summary.csv`：window/model 级聚合和 SLO 结果。
- `batch_replay.csv`：逐请求可见性、批次、延迟、脱敏 hash 和状态。
- `batch_reconciliation.csv`：逐窗口/批次的声明与观测大小、模型一致性和对账结论。
- `replica_health.csv`：逐窗口/replica 的最新遥测、纳入与排除原因、token 容量。
- `capacity_replica_detail.csv`：容量明细的可复核副本，包含节点状态和排除原因。
- `drift_findings.csv`：逐窗口/切片的加权质量差、PSI、有效样本和门禁。
- `tenant_slo.csv`：逐窗口/租户等级的请求、p95 TTFT、错误率和 SLO。
- `cost_report.csv`：逐窗口/模型的输入输出 token 和估算成本。
- `routing_plan.csv`：逐窗口路由状态、稳定/候选比例、原因码和容量。
- `execution_plan.csv`：逐窗口的状态、负责人、回滚目标和时间窗。
- `source_reconciliation.csv`：逐条保留运维备注，并记录正式来源裁决。
- `input_validation.json`：文件行数、重复 id、时间格式和窗口校验。
- `observability_contract.json`：固定低基数 labels、series 上限、counter 和 histogram buckets。
- `run_manifest.json`：输入文件名和 schema 版本，不得包含绝对路径或 secret。
- `audit.ndjson`：只记录窗口、类别和统计数量，不写原始 payload。
- `control_evidence.csv`：每个历史窗口中的许可事件裁决。
- `recovery_options.csv`：每个恢复情景下逐分片、逐配置的可用性及排除原因。
- `recovery_assignments.csv`：满足全部约束的恢复安置，含波次、预热开始与屏障切流时点。
- `recovery_decisions.csv`：每个窗口与情景的可执行或阻塞结论、静态费用下界与实际完成时间。基准情景的结论还必须回写 execution_plan。

## 恢复执行与自检

最终安置必须能够按照 `docs/recovery-operations.md` 的 barrier-wave 协议按时执行。目标是先最小化总费用，再最小化最后一波切流时间。原型的静态选择和 dashboard 预览不能证明完整可执行性。公开字段与枚举位于 `docs/artifact-schema.json`。

```bash
python scripts/check_contract.py --input fixtures --output output
```

这个检查只覆盖公开格式及部分一致性，不给出历史窗口答案或最优安置。应自行验证跨文件业务关系与变化输入，不得修改正式合同、自检脚本或原始输入来取得通过。
