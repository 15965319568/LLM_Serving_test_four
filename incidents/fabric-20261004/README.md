# Numeric pilot replay evidence

This is a synthetic local deployment exercise captured from actual CPU worker
execution of the public starter. It is not a claim of a real customer outage.
The controller booted two workers, admitted three clients, attempted a partially
failing replacement, and retried an earlier operation. No expected-answer table is
included. Values are execution observations, not acceptance rules.

- controller.jsonl retains the controller's ordered request/reply/snapshot records.
  clock_ns is that process's monotonic clock; seq is its local order.
- workers/*.jsonl retains original worker-local event order and original PIDs.
  Generation and invocation connect execution to receipts; public external IDs can repeat.
- collector.jsonl is an additional delivery export with envelope schema collector/2.
  It intentionally arrives in reverse source order and contains a redelivery.
  source/source_line identifies the original event. Delivery order is not event time.
- provenance.json describes capture and transformation. Temporary paths refer to
  this replay's diagnostic environment and need not exist in a later run.

Historical rollout/staged directories cover separate exercises and producer formats.
The live service, controller ledger and collectors observe different lifecycle facts.
Use the published contracts to decide which observations should agree.
