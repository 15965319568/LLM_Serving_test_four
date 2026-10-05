# 多源事故交接与在线推理服务一致性修复

你接手一套基于MLServer的多租户服务。发布演练后，客户回执、遥测、看板和发布控制结论
彼此不一致。维护团队需要一份能够重新生成、逐项追溯的事故交接底稿，以及能持续正确
运行的修复源码。原有基础测试通过不能证明这次事故已解决。

工作包含同一生产链路的离线事实重建和在线行为修复。原始跨系统交接材料在
incidents/handoff-20261005/；旧事故与协议演练材料仍保留。各系统导出使用各自标识，
值班意见未经复核。请依据正式契约和来源记录核实事实，处理矛盾，完成修复和验证。

交付完整修复源码、可运行的reconcile_incident.py、regression_tests/中的新增unittest，
以及根目录REPAIR_NOTES.md。报告含“## 诊断证据”“## 修复取舍”“## 验证”“## 局限”
四个非空章节；新增回归至少1项，全部通过、无跳过。

交接入口使用 --input 材料目录 --output 新目录，生成receipts.json、ingestion.json、
reconciliation.json、assessment.json四份机器可复核文件。正式字段、来源规则、排序及
精度见docs/handoff-contract.md；评分边界见docs/acceptance-v7.5.md及其引用的产品契约。
私有验收会更换同一协议内的材料内容，检查这些产物及在线生产行为；不能只交手填结论。

工作目录/workspace，Linux/Python 3.12，CPU依赖已装好，无需GPU、权重或付费API。
允许自行选择调查工具、批量处理、重构内部实现；不规定调查顺序、工具调用数或修复位置。

基础检查：python -m unittest discover -s tests -v。
新增回归：python -m unittest discover -s regression_tests -v。
交接重放：python reconcile_incident.py --input incidents/handoff-20261005 --output 新目录。
