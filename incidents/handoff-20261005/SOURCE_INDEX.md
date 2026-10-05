# 跨系统交接原始材料

这是作者构造的CPU事故材料，非外部客户数据。各导出保留来源系统自己的标识和格式。
incident.json为交接范围及来源路径；gateway.csv来自网关，route-acks.csv来自控制发布记录，
trace-bindings.csv来自采集关联的批准台账。edge.jsonl.gz、quality.csv、archive.jsonl.gz
为采集源原始导出。配置层和模型制品按profile解释。协议见docs/handoff-contract.md。

目录没有跨系统统一身份表、清洗后事件表或正确发布结论。源系统中的正式ID和原始关联
记录属于核实依据，不能将自报客户端ID、文件位置、当前配置或未批准变更作为统一答案。
旧production-20261005目录属于此前的协议演练材料，不是本目录的关联结果。
