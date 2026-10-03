# 策略来源

正式决策顺序、百分位口径、漂移阈值和指标边界位于 `policies.json` 与 `deployment_policy.json`。实现应读取这些字段，而不是把题面示例或备注里的结论写死。

审批必须同时匹配 candidate model、scope、signed_at 不晚于评估时点，且 revoked_at 为空或晚于评估时点。feature flag、节点状态、事故和回滚时间窗也按各自生效时间截断。`operator_annotations.ndjson` 和 `route_history.csv` 只做 source reconciliation，不得覆盖正式策略；它们可能故意与正式来源冲突。

最终路由状态必须能从正式来源和逐项门禁回算，候选比例不得超过正式策略上限。执行计划中的负责人和时间窗必须来自 `rollback_windows.csv`，不能从聊天内容猜测。
