# 运维接口与验收边界

POST `/admin/activate` 使用管理员 Bearer 凭证，JSON 结构：

```json
{"alias":"chat","spec":{"revision":"candidate","tokenizer":"utf8-v1","context_limit":2048},"expected_epoch":0,"operation_id":"deploy-2026-01"}
```

成功返回200和 activate 的结果；业务错误沿用 JSON error.code/message 与对应 HTTP 状态。
只有管理员可以调用控制接口，普通租户返回403。非法 JSON/字段/参数返回400，
不得触发加载或改变部署。POST `/admin/drain` 不要求正文，成功返回
`{"drained":true}`。先停止新接纳，再等待已接纳工作终止并完成资源清理。

`await Engine.drain()` 不强行取消旧请求，也不关闭 journal。新幂等键返回
draining（503）；已接纳请求的相同幂等重放继续可用；不同内容复用旧键仍为409。
drain 后可以读取 status、事件和 metrics，也可以 close。GET `/health` 此时 ready=false。
`close()` 停止接纳，取消剩余 queued/running 请求，等待所有生成器关闭后卸载后端，
关闭 journal 并释放进程所有权。重复 close 安全；可在 drain/activate 等操作期间 close。
close 完成后，除重复 close 外不再调用该 Engine 实例；重放继续可用的承诺针对 drain
阶段和使用同一 journal 重新启动的实例。

`snapshot()` 保留 scheduler/cache/models 字段，新增 lifecycle：
`{"accepting":bool,"live_requests":int,"generation_tasks":int,"loaded_revisions":[str,...]}`。
后两项内存数量不含历史完成请求。资源清理完成后 live_requests、generation_tasks、
KV 使用和 cache.pinned 均为0；仍被 alias 引用的当前模型可保持加载直到 close。
此接口描述逻辑状态，不要求特定内部数据结构。

验收运行公开兼容测试、控制面并发、持久化迁移、生命周期故障、跨进程恢复、真实
HTTP/SSE、两种后端和多组可复现的事件交错。测试数据与输入顺序会变化，不应硬编码
示例 ID、prompt 或特定 revision。故障注入使用公开 Backend/WorkerEvent 协议。
隐藏验收实现不会提供给解题模型，但不引入文档之外的字段或私有正确字符串。

所有测试使用 CPU，不依赖外网、商业 API 或睡眠耗时来制造难度。计分按完整行为正确性
给通过/不通过，并保留各测试的诊断。解题耗时不设置20分钟限制；项目轮数统计与
功能正确性分别评估，不能在服务里添加等待或让解题模型凑消息数。
