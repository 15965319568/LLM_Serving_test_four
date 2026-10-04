# FleetServe / MLServer operations workbench

这是基于 SeldonIO/MLServer 固定源码的 CPU serving 工程。自有控制层管理双区域租约、
灰度路由、原始遥测、质量回填、发布判定和崩溃恢复；请求经过实际 MLServer registry、
data plane 和运行实例。TokenRuntime是确定性的CPU测试载荷，不模拟神经网络质量。
StreamServe提供持久化SSE、停止词、重试与资源预算。任务见 TASK.md。

## 环境与入口

使用 Linux / WSL、Python 3.12。Harbor镜像已安装固定依赖，且源码直接在PYTHONPATH上；
修改源码无需重装。自行搭建环境时：

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.lock
export PYTHONPATH="$PWD/src:$PWD/services/mlserver"
export PYTHONDONTWRITEBYTECODE=1
python -m unittest discover -s tests -v
python -m fleetserve --state state/demo serve --backend mlserver --demo-workers --port 8081
```

另一个终端发起真实流式请求（公开demo凭证）：

```bash
curl -N http://127.0.0.1:8081/v1/generate \
  -H 'Authorization: Bearer north-local' \
  -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' \
  -d '{"model":"chat","prompt":"Hello","max_tokens":4,"stream":true}'
```

--backend mlserver-parallel 使用 MLServer InferencePool 子进程执行token步骤；
echo/process 是保留的旧兼容驱动。离线CLI操作同一state时必须先停止在线服务。

```bash
python -m fleetserve resolve
python -m fleetserve --state state/replay init
python -m fleetserve --state state/replay begin rollout chat r20261004 --bps 2000 --epoch 0 --operation replay.begin
python -m fleetserve --state state/replay reconcile
python -m fleetserve --state state/replay import incidents/rollout-20261004/bundle.json
python -m fleetserve --state state/replay evaluate rollout --start 100 --end 200 --assessment replay.assessment
python -m fleetserve --state state/replay progress rollout
python -m fleetserve --state state/replay status
```

这些命令用于重放证据，不代表起始版已经满足任务契约。每次新的独立演练使用新的state目录。
生产profile启用逐阶段放量；单窗口评估的PROMOTE不等同于立即全量发布，见docs/progressive-rollout-contract.md。
HTTP管理接口和Python API见契约；python -m fleetserve --help 显示其余运维命令。

## 工程布局与来源

| 路径 | 职责 |
| --- | --- |
| services/mlserver | 固定上游的源码快照、测试、协议、文档和许可证 |
| src/fleetserve | 多区域控制端、gateway、backend适配、遥测、HTTP与运维 |
| src/streamserve | 原有持久化请求协调、SSE、KV与子进程兼容运行时 |
| config/fleet | 显式profile和分层配置；archives为旧离线预览配置 |
| models | 有SHA校验的CPU演练artifact，非大模型权重 |
| incidents | 合成演练原始导出与未核实运维笔记 |
| tests | 可运行的基础兼容检查，不是完整任务验收 |

MLServer来源：https://github.com/SeldonIO/MLServer ，固定提交
 a325e523b5a580d49aeb06e19e73e4a90176b63b，Apache-2.0。详见 UPSTREAM.md。
FleetServe与事故数据为本题原创。没有把上游发布的某个补丁当作本题标准答案。
本部署的新增要求可能超出该上游版本原本的承诺，以根目录契约为准。

上游CPU兼容子集可运行：

```bash
python -m pytest -c services/mlserver/pyproject.toml services/mlserver/tests/test_registry.py services/mlserver/tests/codecs services/mlserver/tests/test_types.py -q
```

可选框架、Kafka集群、Docker和GPU示例不要求运行。上游原始测试保留以便理解兼容边界；
部分内部缓存键断言与本部署新增缓存隔离要求不同，见MLServer契约。
