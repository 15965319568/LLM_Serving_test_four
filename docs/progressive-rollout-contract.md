> 本轮工作见TASK.md，评分边界见docs/acceptance-v7.5.md；本文的完整产品背景不自动增加本轮考点。

# 逐阶段灰度发布契约（v5.1）

生产部署采用逐阶段放量。离线评估给出的 PROMOTE 表示这个观察窗口满足健康标准，
实际发布控制必须同时遵守以下阶段规则。本文规定可观察行为，不规定实现算法。
未配置 policy.rollout 的旧 profile 继续采用 fleet-contract 的单步发布语义。

## 策略与观测接口

可选 policy.rollout 示例：

```json
{"steps_bps": [2000, 5000, 10000], "healthy_windows": 2, "min_window_seconds": 30}
```

steps_bps 为至少两个严格递增的正整数，末项必须为10000；healthy_windows 为正整数，
min_window_seconds 为至少0.001的有限秒数。非法配置应抛 FleetError。
begin 的 candidate_bps 必须等于首项，否则报 invalid_initial_exposure，且没有部分提交。
策略在 begin 时冻结，后续配置变化不得改写既有发布。10000表示完成晋升：候选成为stable，
candidate清空，采用原终态路由格式，不保留10000权重的canary。

`releases.progress(release_id)`、管理员 GET `/admin/progress?release_id=...`、
CLI `progress RELEASE` 返回相同进度；未启用此策略的已知发布返回null，未知发布仍报404。
进度包含 release_id、route_epoch、stage_index（从0起）、healthy_windows（当前连续健康数）、
last_end（本阶段最近一次接受的新窗口终点）、effective_at（本阶段控制端确认边缘ACK的时刻）
和 observations。每条观察保留 route_epoch/start/end/assessment_id/decision/counted；
counted 表示仍参与当前健康累计，历史观察不会因重启消失。

## 窗口与发布

apply 仍先验证评估快照的发布阶段、route_epoch 和 evidence_version。通过的健康窗口
必须满足最小时长；不满足报 short_window。窗口为接纳时间的半开区间 [start,end)。
同一发布、同一路由epoch、同一start/end是同一个窗口，换assessment_id不能再计数；
已接受窗口不能重新贡献已被清零的健康数，重复观察返回HOLD、window_already_observed。
新窗口须按时间推进且不与上一个已接受窗口重叠，相邻边界允许相等，否则报overlapping_window。
这些拒绝均无进度、路由或operation副作用。

健康窗口累计不足healthy_windows时，apply返回HOLD、healthy_window_pending，路由不变。
达到门槛后仅前进一个步骤：中间步骤返回ADVANCE，最后步骤返回PROMOTE；原因均为
healthy_windows_complete。推进仍校验全部租户容量并持久化epoch+1，提交时phase为
terminal_pending（沿用过渡状态名）；中间步骤ACK后回到canary，最终ACK后为promoted。
推进后健康数归零，旧观察不再counted，last_end清空，新阶段需要新的观察窗口。
容量不足时整个操作回滚，不能消耗窗口、占用operation_id或留下部分进度。

初始阶段允许历史事故窗口重放，effective_at为null。后续阶段只有在控制端成功记录
当前epoch的边缘ACK后，才以该次clock()记录effective_at；若边缘已提交但ACK丢失，
恢复时第一次成功记录ACK的时间才是边界，之后重送不得移动它。后续健康/HOLD窗口
start必须不早于effective_at，否则报pre_stage_window。不能把控制库提交时刻冒充边缘生效时刻。

## 迟到修订与回退

当前快照的ROLLBACK优先执行安全回退，不受健康窗口最小时长、重复、重叠及阶段起点门槛
阻止，原因current_regression；仍须满足快照新鲜度及原有容量、事务和幂等约束。
其余有效观察在应用时，必须用最新已接受证据重新判定本阶段仍counted的历史窗口。
历史任一窗口现在ROLLBACK，即使当前窗口健康也须实际ROLLBACK，原因history_regression。
历史窗口现在HOLD（例如质量撤回导致证据不足）会清零旧连续健康数，但不能虚构为退化。
当前窗口HOLD同样清零。重核后如当前是新的健康窗口，它可以成为新的第1个窗口。
已经完成阶段和已被清零的历史不参与后续阶段的健康重核；原始审计仍保留。
无关advisory消息不能改变某个窗口的实际判定，也不能仅因全局证据版本增加就清空计数。
重新核验不得篡改已有assessment快照，也不应生成伪造的用户评估记录。

apply返回原有release_id/decision/epoch/phase，启用此策略时附progress_reason和progress。
operation重放返回第一次持久化的结果，即使之后ACK或阶段已经改变；不得重复计数或发布。
controller.tick、手动apply、人工rollback与重启恢复共同遵守这些规则。不同alias/发布独立。

## 持久化与验收边界

控制库升级到user_version=3，兼容已有v1和v2数据，保留操作、证据、接纳与审计历史。
v3新增rollout_progress和rollout_windows；字段见migrations.py及上述可观察接口。
保留未启用rollout的历史发布，不要求为过去未声明的策略凭空生成观察进度。

验收使用真实CPU进程、SQLite与MLServer。验证器对子用例及其清理设置45秒死锁保护，
模块启动保护60秒、全套保护480秒，Harbor验证总上限仍600秒。卡住的用例记为失败并在
新进程继续其余检查。这是避免无响应实现拖垮整轮的保护，不是Agent作答时限或性能目标。
没有要求逐条执行操作、固定消息次数或固定解题路径。
