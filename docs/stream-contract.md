# 流、取消、重试与资源契约

请求有 queued、running、completed、cancelled、failed 状态；后三者为不可更改终态。
每个已接纳请求恰好有一个 terminal 事件。accepted 序号为1，之后事件序号连续递增；
取消、重复取消、后端完成、后端错误互相竞争也不能重复终态、重复计费或重复释放。
任务返回流终态后，资源清理可能继续异步执行；`drain()` 是清理完成的屏障。

每次向后端发起 generation 都有从1开始的 attempt，重试时严格增加。
只接收当前 attempt 的事件，旧 attempt 的 delta/finish 必须忽略。
当前 attempt 的非终止 token index 从0连续增加；已经接收的旧 index 为重复投递，
应忽略。index 出现空洞，或 finish 的 index 不等于下一个 token index，终止为
failed/backend_protocol。finish_reason 只允许 stop、length；非法值也为 backend_protocol。
正常后端在 max_tokens 内结束；若第 max_tokens 个 token 后尚未 finish，服务应主动
以 completed/length 结束并关闭生成器，不能接收额外 token。

`BackendFault(retryable=True)` 且本请求还没有接受过任何当前 attempt 的 token 时，
应当重试一次且最多一次，始终使用接纳时的 revision。不可重试异常，或者第二次故障，
终止 failed/backend_error。任何已接受 token（即使还被停止串前缀暂存）都禁止重试。
重试不得重复接纳、重复 KV 预留或产生第二个 accepted。生成器无 finish 提前 EOF，
终止 failed/backend_eof。取消中止任务，reason 为 client_cancel。

这一隔离同样适用于进程协议：worker 的 event/error/end 响应和 cancel 命令都应带
request id 与 attempt，旧 attempt 的结束或错误不能结束新 attempt。JSONL 仍保留
原有 id、event/error/end 字段，增加顶层 attempt；generate 的 Work 已包含 attempt。
`ProcessBackend(worker_module=...)` 可加载遵守此协议的故障注入 worker 模块。

完成时 flush 尚未匹配成停止串的前缀；失败或取消不泄漏暂存前缀。已输出的 delta
保留在 journal 中。terminal usage 是已接纳 prompt 的 token 数和实际接受的生成 token
数；重复、陈旧和无效事件不收费。TTFT 从接纳到第一个有效生成 token，只记录一次。

`stream(ticket, after)` 返回 seq > after 的后续事件。after 必须为0或一个已存在的事件
序号；负数、非整数和超出当前最后序号返回 invalid_cursor（400）。两个读取者可用
不同速度、不同 cursor；断开或取消一个读取者不取消推理，不影响其他读取者。
慢客户端不能阻塞推理和其他客户端，重放从持久化事件读取，活动内存不能随读取者
落后程度无限累积。结束后在最后一个事件处重放得到空流。

缓存至少按 tenant、revision、tokenizer 和 token 内容隔离。活动缓存引用不能被其他
请求驱逐；取消和重试要保持引用平衡。排队取消不产生缓存引用或后端调用。
调度器、缓存、后台任务、后端生成器的活动资源，应在 drain 后归零；历史 journal
允许保留，但不能让已完成请求一直占用内存任务、请求对象或缓存 pin。

对其他租户的 status/cancel/stream 一律返回404；ticket 中伪造 revision 不得改变
持久化绑定或泄漏数据。鉴权失败不写入请求 journal，不增加 accepted 指标。
