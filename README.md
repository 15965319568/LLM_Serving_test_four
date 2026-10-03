# LLM Serving 发布观测与分批恢复

这是 Harbor 任务的公开初始代码。需要修复历史证据重建、服务指标、门禁以及联合安置与预热切流规划。完整可见合同在 TASK.md 和 docs/；任务运行时提供合成 fixtures。

```bash
export PYTHONPATH=src
python -m inference_observer --input fixtures --output output
python scripts/check_contract.py --input fixtures --output output
```

公开自检只验证格式与基本一致性。最终验收还会独立重算历史业务语义、全局最优性与同 schema 输入变化。
