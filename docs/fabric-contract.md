# Numeric inference fabric deployment contract

This Linux/Python 3.12 profile hosts deterministic CPU MatrixRuntime models in two
real MLServer worker processes. It supplements the historical stream and rollout
profiles, whose documented APIs remain required. The runtime exercises serving;
it does not simulate neural model accuracy. No GPU or external model is needed.

## Service surface

Start with `python -m fleetserve.inference --config config/fabric.json --state state/fabric --port 8082`.
One owner may open a state directory at a time; ownership is released on close or
process death. Persistent state survives restart. Demo credentials are public.

| Endpoint | Identity | Behavior |
| --- | --- | --- |
| GET /health | public | `{ready: bool}` reflects admission availability |
| POST /v2/models/{alias}/infer | tenant bearer | MLServer V2 JSON request/response |
| GET /v2/receipts | tenant bearer | `{requests: [...]}` for this tenant only |
| POST /admin/deploy | admin bearer | publish a model specification |
| POST /admin/flush | admin bearer | `{alias}` dispatches buffered requests and awaits physical batches |
| GET /admin/fabric | admin bearer | deployments, real worker PIDs, accounting, recovery_errors |

The Python surface is `ServingFabric(state_dir, workers=2)` with async start, deploy,
infer(tenant, alias, InferenceRequest), flush(alias), batching(alias), drain and close.
deployment(alias) returns a detached snapshot; journal.receipts(tenant=None) and
journal.accounting() expose the ledger locally. Close is safe to repeat. Drain
rejects new inference/deployments while letting existing work settle.

Authentication precedes mutations. Invalid credentials produce 401; tenant access
to admin paths, admin access to tenant paths and a request parameter tenant that
disagrees with the authenticated caller produce 403. Unknown alias is 404; invalid
bodies/shapes/fields are 400; epoch/operation conflicts are 409; runtime failures
are 503. Error JSON has error.code; detailed prose is free. Client socket closure
cancels that inference wait and records a cancelled receipt.

## Execution and batching

Supported inputs are nonempty FP64 heads with flat numeric data, unique head names,
positive dimensions, equal leading row counts across heads, and data length equal
to the product of the shape. The leading dimension is a caller's minibatch length.
Different requests can have different lengths. Invalid requests must not damage
neighbors or create a completed receipt; shape/auth rejection before admission
creates no receipt.

The runtime calculates `value * factor + offset` for every element. Factor belongs
to the deployment; request offset defaults to zero. Input x produces FP64 x_out with
the same shape; optional outputs selects output names. Per-output scalar unit is score.

Compatibility requires the same live model generation, tenant, input head names,
datatypes, nonbatch shapes, input parameters, requested output set and parameters,
and execution parameters. Transport id and headers Ce-Id/Ce-Requestid do not separate
execution; other parameters and headers do. Head order does not change compatibility.
Requests with different compatibility must never share a model call. Original input
objects remain unchanged.

Within an open batch window, compatible requests present before explicit flush merge
up to max_batch_size request members, regardless of each member's row count. Ordinary
deadlines also dispatch work without flush. Disabling batching or dispatching every
compatible concurrent request separately violates the profile. Batch size 1 permits
single-call execution; no batching is required for historical streaming. Both real
replicas must execute; normal sequential traffic uses both. Replica selection is free.

Responses preserve each caller's external ID and row order/shape. IDs may be absent
or duplicated and are not internal unique identities. Fabric responses contain the
public alias/revision and parameters epoch, receipt_id, worker_pid, invocation,
revision. PID/invocation come from actual runtime execution: one physical call has
one invocation shared by its members; different calls have different invocations.
Response objects are independently mutable. Scalar output metadata broadcasts intact;
list output metadata has one element per row and is sliced with rows. A malformed
response (missing outputs, wrong dimensions/data length, duplicate heads or wrong
row-metadata length) fails all remaining members and must not poison later batches.
Historical merge helpers remain for compatibility; their old last-value-wins and
list-concatenation semantics do not authorize mixing incompatible live requests.

Cancellation before dispatch excludes that member from runtime input. After dispatch,
it must not cancel shared physical execution or poison neighbors. Even if all callers
cancel, resources remain valid until physical work ends. Cancellation and duplicate
or late worker replies cannot settle a request twice. AdaptiveBatcher flush/close/stats
remain available: close rejects new requests and waits physical work; stats exposes
queued, waiting, running_batches, dispatched_batches, closed. Internal structures
and synchronization strategies are not prescribed.

## Publication and recovery

Deploy fields: alias, revision, finite numeric factor, operation_id, expected_epoch
(default 0), positive max_batch_size (4), positive max_batch_time in seconds (.02),
runtime_options object. IDs follow Fleet identifier rules. An absent alias is at
epoch 0; success advances it once and returns alias/epoch/generation/revision. Every
new accepted specification gets a distinct opaque generation even when its public
revision name is reused.

Publication is atomic and durable across public APIs. Unprepared replicas cannot
receive candidate requests. Failure on any replica preserves the old usable deployment.
Requests admitted before publication retain their generation through queues, physical
batches, responses and accounting. Requests after success use the committed spec.
Preparing a candidate must not stop the old deployment serving; old physical calls
can finish after the new deployment becomes available.

Operation IDs bind the complete deploy request, including expected epoch, batching
settings and runtime options. Exact retries return the original committed result
even after later updates/restart; changed requests conflict. Different concurrent
operations on one expected epoch have at most one winner. Failed/cancelled preparation
commits neither operation nor route and is retryable. The optional Python-only async
after_prepare hook runs after preparation and before publication for fault exercises;
its exception/cancellation has the same no-publication guarantee. It is not an HTTP
field. No particular lock, transaction layout or repair location is required.

replicas.pids() lists real owned workers; replace_worker(pid) kills one owned worker
and waits for a restored replacement. Unsolicited worker death is also detected. A
request whose executing worker dies fails once; silent re-execution is not authorized.
New workers replay successful serving specifications, including factor and batching
settings. Failed candidates must not enter recovery. Restoring workers are ineligible
for prediction until ready.

## Monitoring and diagnostic evidence

Each admitted request gets an independent receipt_id, not derived from external ID.
Receipts include tenant, alias, external ID, admission epoch/revision/generation,
row count, state (active/completed/failed/cancelled), timestamps and optional execution
PID/invocation/error. Admission attribution is immutable; terminal state is final.
Accounting groups by tenant/alias/epoch/revision. Admitted equals active + completed +
failed + cancelled; completed_rows sums completed minibatch rows, not shape products.
Physical batch success and logical cancellation are distinct facts.

Restart marks old active receipts failed with controller_restart without rerunning
them. Terminal receipts and operation identities survive. One owner's concurrent
state access by another controller must be rejected.

Runtime options events (directory), load_gate/predict_gate (paths whose creation
releases a barrier) and fail_pid (a real worker PID rejecting load) enable repeatable
fault exercises. Per-PID JSONL events describe actual loads/calls/unloads with local
file order, kind, generation, PID and where applicable invocation, rows or active count.
Collectors may reorder and repeat deliveries; merged-log line order is not a global
execution clock. Tests release gates in cleanup. These are local exercise controls.

Scope is CPU process death, async concurrency and persistent files; GPU kernels,
distributed consensus, host power-loss durability and model accuracy are out of scope.
Acceptance changes names, shapes, factors and interleavings, and checks observable
behavior and actual execution rather than a required code structure.
