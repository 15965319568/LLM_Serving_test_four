# V7.4 验收范围

本文件只界定评分边界。业务规则以本节指定的产品契约和生效配置为依据，历史任务清单
不增加评分条件。内部命名、表结构、schema版本、文件布局及锁的选择均不属于验收要求。

| 范围 | 正式产品依据 | 验收观察 |
|---|---|---|
| 配置与发布口径 | fleet-contract 的profile及policy规则 | 选定production层、生效数组及嵌套值；已开始发布的阈值 |
| 数值请求及在线部署 | production-contract 的接纳/执行；fabric-contract 数值批处理 | 数值、ID、接纳epoch、真实worker调用和行数、取消终态、旧实例安全退役、新发布进度 |
| 原始文件到有效证据 | production-contract 的交付协议；evidence-contract 的格式/来源/适用人群 | CSV/JSONL/gzip导入、重复/非法行处理、持久保留事实与有效样本分别核对 |
| 监控和发布判断 | evidence-contract 的统计及完整性规则 | sample_ids、每cohort的count/quality_count/error_rate/p95_ms/quality_mean，以及HOLD/PROMOTE/ROLLBACK |
| 恢复及回滚 | production-contract 的after_apply和幂等；fleet-contract 的发布 | 采集生效但未确认时退出后重放；历史完整响应、当前稳定部署及接纳守恒 |
| 修复说明与新增回归 | 本文交付约定 | 四章节非空；子进程实际发现并运行至少1项unittest，全部通过、无跳过 |

## 本轮输入边界

使用既有 Fleet、ProductionService、ServingFabric(workers=2)、DeliveryInbox 的公开API；
配置中的 north/south 分属 interactive/batch，east/west 区域，各有稳定/候选 revision。
数值载荷为MatrixRuntime的单输入x、FP64、shape=[n,1]，n为1至3；factor、输入和offset
可为有限负数。每组兼容请求在批窗口内填满不超过3的max_batch_size。外部ID与跨租户key
允许复用。API、运算和响应字段遵循既有公开契约。

已有runtime_options.events和predict_gate作为公开诊断观察点：JSONL中kind/pid/version、
started/finished的invocation/rows、unloaded的active须如实反映worker；replicas.pids()列出
真实进程。验收覆盖旧预测被屏障保持时的在线替换与候选发布、整批或部分成员取消。
合理等待保护用于防止挂起，不以吞吐量或机器速度评分。

采集使用import_manifest以及receive/reconcile；输入包含带BOM的CSV、JSONL、gzip、
空行和独立坏JSON行，delivery/1信封和event-v2/export-v1业务记录，包含可选字段合法缺失。
分片文件顺序、记录顺序及重复次数可不同。配置登记来源格式/角色/权威，业务记录还声明
sample_kind或revision；这些字段的作用和适用范围见evidence-contract。
原始保留对象通过export_raw公开查询，按producer/event_id保留最新合法修订及冲突/撤回状态；
过滤出评估人群不等于销毁原始证据。坏业务记录不成为合法保留事实，重复不增加事实数量。

评估窗为接纳时间半开区间，迟到回填、冲突、撤回、更高修订、来源和单位差异按产品规则
处理。本版同时核对每个cohort的具体统计，不能只返回决策标签。上游测量回填是普通输入，
不将机器实际运行速度当成固定延迟预期。数据文件可改变数量、数值、来源组合和到达顺序，
但不会引入本文和引用契约未公布的协议或业务条件。

## 范围外与交付

只检查表中链路，不要求修复逐阶段rollout、任意推理进程退出、旧库迁移、所有历史profile、
完整HTTP/SSE、GPU性能或真实LLM精度。文档中的历史扩展描述不自动进入本轮评分。
公开源码保留成熟工程与兼容路径；本轮没有新增审计产品、求解器或额外后台服务。

REPAIR_NOTES.md四个章节须存在且有内容；文字应记录真实依据、取舍、验证及限制。
自动检查不设字数，也不能代替对文字真实性的人工审查。regression_tests最初无测试用例，
新增test_*.py须被unittest发现，至少1项、全通过、无跳过。回归检查证明实际可执行，
不声称穷尽测试质量。

两组检查为交付物与联合生产链路。全部通过得1，否则0。业务组含多份原始材料情形，
每个子场景按因果顺序断言，前置失败时其后续步骤不会继续。全部私有Python/shell评分
代码不超过100物理行，含控制、断言、导入与空行；普通数据文件无可执行测试或测试DSL。
不规定工具调用数、排查顺序或是否批量处理。难度指标另以本版真实五测核定。
