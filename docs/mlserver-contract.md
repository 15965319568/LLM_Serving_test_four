# MLServer 集成契约（v5.1）

本工程以固定上游源码作为实际执行依赖。不能替换为空实现、绕过到Echo或仅修外围输出。
允许修vendored核心。保留MLServer类型、registry、DataPlane、parallel和原有公开API；
无需升级上游、训练模型、安装GPU框架或联网推理。以下是本部署的新增要求，不宣称每条
都是原上游版本曾承诺的行为。

`fleetserve.backends.mlserver.MLServerCluster(parallel=False,workers=1)` 作为backend_factory，
native路径真正经过 MultiModelRegistry、DataPlane.infer_stream 和 TokenRuntime；
parallel路径真正经过 InferencePool 子进程、ParallelModel.predict 和DataPlane.infer。
TokenRuntime为CPU测试载荷，输出revision:index，response.parameters.worker_pid可证明进程边界。
ParallelModel本身不承诺原生流式传输，适配器逐token调用并提供对外流式接口。

`DataPlane(settings,model_registry,metrics_registry=None)` 支持独立Prometheus registry，
默认仍是原全局registry。这是嵌入多个服务实例所需的公共扩展。
缓存必须同时区分解析后的模型名、实际版本、实例替换代次和请求内容；默认版本请求
与显式版本请求不能串结果，同版本reload也必须使用新实例结果。仍支持 cache_enabled
及模型级禁用，命中响应ID遵守原协议；不要求特定内部缓存键字符串。

一个被接纳的infer或infer_stream持有本次解析的运行实例直到成功、失败、取消或显式
关闭。期间热替换不能物理unload该实例；新请求使用加载成功的新实例。替换抛错或
返回not-ready时不能使原ready实例退役。显式unload停止新接纳，等待现有调用结束后清理。
reload可以在旧实例退役后才返回，也可以先返回；无论采用何种方式，并发新请求必须能在
旧流仍打开时访问已成功加载的新实例。不能要求客户端先关闭所有旧流才发布新实例。
空输入流正常结束；显式关闭/取消返回或传播完成前，必须完成后端生成器finally清理，
不能依赖稍后的垃圾回收。可以扩展内部registry接口，
不规定锁或引用计数实现。并行worker执行请求也需满足在途实例的生命周期保证。

infer计量归属实际解析的version，默认请求不能只标空version。成功完成计success，
失败、取消和提前关闭计failure，duration覆盖实际调用生命期；不计未开始的调用。
客户端停止读取与工作线程晚到响应不得导致InvalidStateError或使后续请求失去响应。

并行响应允许取消后的迟到与重复消息，须安全忽略；同一message只能完成一次。
发送可以即时产生响应，响应必须可被正确关联。worker停止时在途请求得到失败，
其余worker可继续；内存关联在完成、取消、发送失败后清理。WorkerRegistry中的
name/version是二元身份，含连字符的合法名称不能碰撞。失败reload不能丢失既有恢复记录；
成功同version替换后记录指向新设置，不能因旧实例清理而卸载新实例。

上游原测试完整保留，含可选框架/容器依赖的测试不属于本任务的CPU验收范围。
本部署改变缓存键语义；原测试中直接按旧内部键读取cache的断言不作为兼容规范，
真实推理响应与缓存隔离仍须验证。README给出CPU可运行的上游兼容子集命令。
