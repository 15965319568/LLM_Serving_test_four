# 联合生产流程原始观测

本目录来自公开起始工程的真实 CPU 演练，不是客户生产数据，也不是标准答案。
运行源码入口是 fleetserve.production.replay；种子和字节摘要见 provenance.json。

raw/admissions.csv 保留接纳导出；raw/client.jsonl 是请求与响应；raw/terminal.jsonl 是终态
导出；raw/quality-review.jsonl 为独立 CPU 载荷复核；raw/workers 为原始进程记录；
raw/controller.jsonl 为当次控制与采集返回值。后者也可能受起始版缺陷影响。

capture.json 列出滚动采集分片，partitions.json 登记生产者来源。deliveries 保留不同格式、
重叠、重投和采集损坏。offset 是分片内来源位置；文件顺序是交付次序。复核数据中同时
有临时导出和后到修订；记录是否能进入决策由公开契约决定。

不提供统一清洗总表。原始 worker 文件末尾包含资源释放记录。其他事故目录来自独立
历史演练，不应因时间戳接近就直接拼成同一次执行。
