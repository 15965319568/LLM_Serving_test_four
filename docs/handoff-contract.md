# 事故交接输出与来源协议

本协议规定可复核交接的业务事实与文件格式，不指定内部表、对象或实现方式。
incident.json给出业务alias、stable/candidate、接纳窗口[start,end)、取证截止cutoff和各来源路径。
时间均为Unix秒，cutoff是身份绑定和路由台账的取证基准，不是模型运行时间。遥测是随后
收集的回填，finished_at可以晚于cutoff；事后批准不改写该基准下的身份关联。fleet_manifest
指定生产profile；其中模型制品、来源rank/role、冻结policy和原有证据规则继续适用。

## 独立来源

gateway CSV是已接纳请求日志。admission_id为正式身份；(boot,ticket)在本材料中唯一，
ticket和external_id可在其他启动实例或租户重复。accepted_at是接纳时间。route_receipt
引用控制侧回执，不能用当前alias、客户端model名称或遥测自报版本补写实际执行版本。

route-acks CSV按receipt记录准备和发布。只有state=published且recorded_at<=cutoff的行
对本次交接有效；同receipt采用有效行中最大sequence的tenant/alias/revision/cohort。
最高有效sequence的重复同值只算一次。材料不含该层最高有效sequence相互冲突的情况。
路由tenant须与接纳tenant一致。没有有效发布回执的接纳不进入receipts.json，仍可被
遥测关联到，并应在该条遥测的裁决中标为unpublished_route。

trace-bindings CSV是各采集源的关联变更台账。绑定身份为(producer,tenant,trace_id)，
定位目标为(boot,ticket)。trace_time属于[valid_from,valid_to)时，该绑定才适用；空valid_to
表示无上界。status=draft未获批准；approved和revoked须approved_at<=cutoff才对该次
交接有效。在该身份、该有效时段和知识截止内，最大binding_revision决定关联事实。
同最高修订的(status,boot,ticket)不同表示binding_conflict，相同表示重复。revoked
阻断旧关联；更高的approved可重新批准。较低修订或不适用时段不得覆盖有效较高修订。

观测文件每条为observation_id、producer、tenant、trace_id、trace_time、payload。
JSONL允许BOM、空行、gzip；坏JSON行独立报告，其他合法行继续处理。CSV也允许BOM，
payload为JSON对象文本，trace_time转数值。材料中的CSV结构和payload JSON语法合法。
observation_id为源导出记录身份：完整内容相同的重复只裁决一次；同ID不同内容为
observation_conflict，不能擅取首条或末条。这与payload内部业务event_id/update_seq的
版本规则不同。同一次交接中观测记录不可被修订，业务修订必须是新的observation_id。

绑定目标须存在于gateway日志且tenant一致。payload采用producer登记的event-v2/export-v1
协议，数值合法性及可选字段遵循evidence-contract。绑定后将request_id或request设置为
正式admission_id，再判断业务载荷合法性。保留其余字段和原始单位；sample_kind、revision
声明和权威等级在后续评估中生效。合法shadow、合法缺字段和tombstone仍是可保留事实。
本材料producer均已登记；网关必填值合法、正式admission_id唯一。事件仍可能有非法业务值。

## 命令及四份产物

python reconcile_incident.py --input DIR --output NEW_DIR

入口须能在新输出目录重建结果，不依赖先前运行的缓存或人工编辑。可重构所有内部代码，
只要求该入口和以下外部格式稳定。JSON对象键顺序、空白、文件编码BOM与否不评分。
文件使用UTF-8，字段采用下表，列表按指定业务键升序；不能遗漏无效或无结果的观测。

| 文件 | 结构 |
|---|---|
| receipts.json | 数组，每项request_id/tenant/alias/revision/cohort/admitted/kind/source，source=gateway；包含所有有有效发布回执的接纳，按request_id排序 |
| ingestion.json | 数组，每项observation_id/producer/record；record为绑定身份后的合法原payload，含全部保留字段，按observation_id排序 |
| reconciliation.json | 对象rows/parse_errors；rows按observation_id排序，parse_errors按file、line排序 |
| assessment.json | 对象decision/metrics/sample_ids；metrics按既有evidence-contract定义，sample_ids升序 |

rows每项为observation_id、disposition(retained/quarantined)、reason、admission_id。
retained使用reason=bound和正式ID；quarantined的admission_id为JSON null。原因如下：

| reason | 可核验事实 |
|---|---|
| observation_conflict | 同源观测ID出现不同完整内容 |
| unresolved_binding | 没有适用于本次知识截止和trace时刻的批准绑定 |
| binding_conflict | 最高适用绑定修订存在不同关联事实 |
| revoked_binding | 最高适用绑定状态为revoked |
| missing_admission | 绑定指向的网关启动实例/本地编号不存在 |
| cross_tenant | 绑定指向了其他租户的接纳 |
| unpublished_route | 接纳无法取得匹配租户的有效发布回执 |
| invalid_payload | 已正确关联，但业务载荷违反登记协议 |

一个观测同一时刻可能只能核实到其中一个边界；以上原因按表列出的事实依赖优先处理。
parse_errors每项file、line、code，file为incident.json中的相对文件名，line从1计数，
code=invalid_json。无法解析的行不捏造observation_id，不进入rows。重复导出不增加rows。
retained行与ingestion项按observation_id一一对应，绑定ID必须能在receipts中找到。

材料完整性声明：本轮来源包是截止时刻的完整导出。在判定有效观测、隔离异常并形成
完整裁决表后，按required_sources/required_cohorts声明窗口end的完整性。缺失质量样本
仍不允许补健康。assessment使用本profile创建的发布观察状态、spec中的stable/candidate
和[start,end)窗口，套用正式证据规则。浮点统计保留6位小数（数值round到6位即可），
无需固定小数字符串；整数计数保持整数，空统计用null。四份文件不含运行时随机ID或时间。

正式范围包含数量、数值、文件顺序、重复次数及绑定有效记录变化后的材料。同协议的
不同输入必须重新计算，不以公开事故的一组固定输出代表所有输入。不要求固定SQL、缓存、
模块布局，亦不要求用指定函数才能得到结果。报告还应说明调查依据和实际验证。
