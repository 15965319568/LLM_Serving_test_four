# Revision 热切换契约

一个 alias 指向一个不可变 ModelSpec 和单调递增 epoch。初始 epoch 为 0。
ModelSpec 的 revision、tokenizer、context_limit 一经注册不可改变；同 revision
使用不同参数返回 `revision_conflict`（409），即使它已经退出流量。
多个 alias 可以合法指向同一个 revision；同 revision 不重复加载，不提前卸载。

`await engine.activate(alias, spec, expected_epoch, operation_id)` 返回：
`{"alias": alias, "revision": spec.revision, "epoch": new_epoch}`。
每次成功操作 epoch 加 1，包括切换到当前相同 revision。alias 必须已存在；
expected_epoch 必须是非负整数（bool 不算整数），operation_id 是非空字符串且不超过128字符。
参数非法返回400；alias 不存在返回 `unknown_model`（404）；比较失败返回
`epoch_conflict`（409）。不能直接接受“比当前大”的 epoch 以覆盖并发更新。

新后端必须先完成 load 才能成为当前部署。加载失败返回 `load_failed`（503），
原 alias/epoch、既有请求和持久化部署不变。加载期间旧部署继续接纳和运行请求。
同一 alias 的并发竞争最多一个比较成功；未获选且没有其他所有者的已加载版本应被清理。
允许按控制操作串行化，但不能让旧流量等待后端加载。

operation_id 在整个控制面唯一。完全相同参数的重复操作，包含 expected_epoch，
返回首次成功结果，不再次加载或增加 epoch；改变任一参数返回
`operation_conflict`（409）。成功结果跨重启保留；失败操作不占用 operation_id。

请求在**接纳**时取得当前 ModelSpec，包括 tokenizer 和 context limit；之后即使仍在
排队，也使用该版本直到终止。相同幂等请求在切换后仍绑定原请求。新幂等键使用新版本。
旧版本在任何 alias 仍引用它，或存在已接纳但未终止请求时，不得卸载。
最后一个这样的所有者释放后，关闭后端生成器、释放缓存引用，再回收旧 worker 和
该 revision 的无引用缓存。中途断开的读取客户端不决定模型版本的生命周期。

`catalog.list()` 与 `snapshot()['models']` 每个条目增加 `epoch` 字段。
系统停止接纳后不能再执行新的热切换，返回 `draining`（503）；已经成功的操作幂等
重放仍应返回原结果。后台准备尚未提交就遇到 drain/close，应回滚准备并返回503。
