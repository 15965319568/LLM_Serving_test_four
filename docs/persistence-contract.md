# 持久化与恢复契约

故障范围为服务进程异常退出或被 kill，宿主操作系统及文件系统继续工作；不要求
模拟整机掉电或磁盘损坏。SQLite WAL 的正常事务持久化语义可用。

现有 SQLite user_version=1 的 requests/events 数据库属于兼容输入。
升级不可删除或重编号已有请求、已提交事件或幂等键；保留已经完成请求的原始输出
和 usage。允许增加列、表和 schema 版本。`fixtures/legacy-v1.sqlite3` 可用于手动验证。
不能只支持全新空数据库。

新版本的 alias、epoch、不可变 revision 定义和已成功 operation_id 需要原子持久化。
进程在控制操作成功返回之后退出，再启动时以持久化部署为准；配置文件提供初始
别名和租户/资源参数，不能把已部署版本静默改回配置初值。

同一 journal 只允许一个活动 Engine 所有者。第二个所有者必须返回
`journal_in_use`（409），不能修改数据或抢走运行中请求。正常 close 和进程被杀后
都应释放所有权，允许重新打开。不依赖不可恢复的永久锁文件标记。

重启时，历史 completed/cancelled/failed 请求及其事件保持原样；queued/running
请求统一转为 failed/server_restart，只追加一次 terminal，保留已经提交的 delta
和 usage。不能再次调用 backend.generate，也不能把部分输出拼接到新的 generation。
这一规则对 v1 和新 schema 都适用，重复重启不得继续追加 terminal。

`submit` 的幂等性是 (tenant,key) 加原始 Request 的规范化指纹，模型 alias 是指纹的一部分，
当前解析出的 revision 不是指纹的一部分。相同请求无论重启或切换都返回原 request_id
与 revision；不同原始请求返回409。同一次并发接纳只能留下一个请求和一个 accepted。

请求事件、输出 token 计数和终态保持事务一致，不能出现“终态已提交但缺少 terminal”
或“重放有 token 但 usage 未计入”的状态。重启后的 accepted、terminal、tokens 计数
与 journal 对账；TTFT 从新增持久化采样恢复，v1 没有该采样的历史记录不补造延迟。
不能把 prompt、幂等键、请求 ID、访问凭证加入 Prometheus 标签。
