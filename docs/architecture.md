# 原有架构与兼容边界

`asgi.Application` 绑定租户凭证并生成 `Request`；`Engine.submit` 完成校验和接纳，
把固定 token 预算交给 `Scheduler`。后台 pump 把可执行请求送给后端，`PrefixCache`
为活动请求持有前缀引用。`Journal` 为每个请求记录 accepted、delta、terminal 事件；
`Engine.stream` 用 SQLite 序号提供重放。`StopFilter` 在 token 边界之外匹配停止串。
`Metrics` 只接收已注册模型别名和有限 outcome，不接收任意请求标识。

`EchoBackend` 和 `ProcessBackend` 都提供异步 `load(spec)`、`unload(revision)`，以及
`generate(Work)` 异步生成器。进程后端通过 JSONL 多路复用，每个 revision 一个 worker。
`HTTPServer` 是用于集成测试和本地部署的标准库 HTTP/1.1 主机；ASGI 应用也可放入
外部主机。此项目未宣称兼容完整 OpenAI API。

请求路径是 `/v1/generate`，不是 chat completions。非流式接纳返回 202 和 request_id，
GET `/v1/requests/{id}` 读取状态；GET `/v1/requests/{id}/events` 读 SSE；DELETE 取消。
`Idempotency-Key` 必填；Authorization 决定 tenant，请求正文不得自行指定 tenant。
GET `/metrics` 需要管理员凭证；GET `/health` 不需要凭证。

调度是有限租户集合上的加权循环服务，不要求全局最优调度。排队不占 KV；活动请求
保留 prompt token 数加 max_tokens 的预算。不能用取消其他租户请求的方式腾容量。
超出上下文或总 KV 的单请求在接纳前失败，队列满返回 429。

`unicode-v1` 把 Unicode code point 作为一个 token；`utf8-v1` 按 UTF-8 字节。
后端每个有效非结束 WorkerEvent 是一个生成 token，不等于字符数。停止串可能跨
多个 token；前缀暂存和停止串消耗的 token 也计入 completion_tokens。
前缀缓存可提升相同租户、revision、tokenizer 和 prompt 的后续请求命中。

已有公开测试全部应继续通过，包括 POSIX 子进程与真实 HTTP socket 测试。
可重构内部文件；不以特定算法、类名或源代码文本匹配作为功能验收标准。
