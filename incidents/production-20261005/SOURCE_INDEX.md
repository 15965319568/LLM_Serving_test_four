# 生产演练交接材料

这些材料来自本仓库CPU演练的作者构造事故包，不是外部真实用户数据。
文件保留各系统格式、重复导出、未完成传输与历史语义；没有预先清洗为标准答案。

| 材料 | 来源及用途 |
|---|---|
| manifest.json、config.json、production.json | 发布配置层及profile选择；取值按正式配置契约解释 |
| admission-export.csv | 网关已接纳身份，时间为Unix秒；用于关联业务事实 |
| gateway-final.jsonl | 网关终态交付，补全edge分区0至11号运输记录 |
| capture-*-measurements.* | 测量系统回填，字段单位随记录携带 |
| capture-*-q-fast.*、q-slow.*、legacy.* | 不同采集渠道的质量回填及历史格式 |
| capture-*-secondary.*、archive.*、notes.* | 辅助来源、历史载荷及看板导出，来源登记见production.json |
| delivery-0/1/2.json | 三批文件交接单；顺序表示到货批次，不是窗口归属或修订优先级 |
| operator-notes.md | 值班人员的当时说法，未作审核；不替代正式配置和产品契约 |

各渠道对同一外部ID的说法不一定描述同一业务事实。接纳与采集、运输确认与事实修订、
当前配置与既有发布分别由其正式契约解释。材料不保证初始实现已经遵守这些契约。
不要求相信某份看板或把异常记录统统删除；应保留可复核的来源关系和原始记录。

可以通过既有operations.imports.import_receipts导入接纳CSV，再用DeliveryInbox.import_manifest
重放交接单。在线CLI及原有完整replay入口见README；使用新的状态目录，避免污染旧演练。
本包只包含交接导出，不包含全部在线控制日志；推理生命周期问题还需从现有代码与复现核实。
