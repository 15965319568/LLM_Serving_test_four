# StreamServe workbench

This repository contains an original, runnable Python inference-serving workbench.
It has tenant admission, KV reservations, prefix cache leases, incremental stop
matching, a SQLite request journal, SSE replay, an ASGI application and a real
subprocess worker transport. The CPU drivers model a token-producing worker;
they do not implement neural network inference or require a GPU/API credential.

The current implementation is a **single-deployment** service. The engineering
assignment in `TASK.md` adds reliable live revision changes and restart semantics.
Existing behavior is covered by runnable compatibility tests. No published issue
or patch is used as the task source, and no implementation was deleted from an
upstream repository to manufacture the starting state.

```bash
export PYTHONPATH="$PWD/src"
python3 -m unittest discover -s tests -v
python3 -m streamserve --config config/demo.json --backend process --port 8081
```

Example from another terminal:

```bash
curl -N http://127.0.0.1:8081/v1/generate \
  -H 'Authorization: Bearer demo-token' \
  -H 'Idempotency-Key: hello-1' -H 'Content-Type: application/json' \
  -d '{"model":"chat","prompt":"Hello","max_tokens":4,"stream":true}'
```

Demo credentials are public local test values. No paid inference endpoint is used
inside the workbench. Use Linux/WSL for subprocess and server tests. Runtime and
tests use only Python's standard library; editable installation is unnecessary.

Architecture and compatibility: `docs/architecture.md`.
New behavior: `docs/revision-contract.md`, `docs/persistence-contract.md`,
`docs/stream-contract.md`, `docs/operations-contract.md`.
Baseline fault observations: `fixtures/observations/README.md`. These are locally
captured inputs and outputs, with no root-cause analysis or repair implementation.

Public tests document compatibility, not complete acceptance of the new feature.
You may add tests and refactor modules. Preserve the documented public interfaces.
