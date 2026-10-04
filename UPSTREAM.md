# Upstream provenance

Project: SeldonIO/MLServer — https://github.com/SeldonIO/MLServer

Pinned source commit: a325e523b5a580d49aeb06e19e73e4a90176b63b

License: Apache-2.0; the upstream LICENSE and copyright notices are preserved in
services/mlserver. This is a source snapshot, without its .git database, bytecode,
build products or optional example model binaries. Exclusions are listed in
services/mlserver/SNAPSHOT_EXCLUSIONS.json. They are not part of the CPU runtime.

The initial local embedding extension allows DataPlane to accept an optional
metrics_registry. Original tests, source, protocol definitions and documentation
are retained. Local task requirements live in root docs/ and do not claim to be
the upstream project's historical guarantees.

FleetServe, StreamServe, configuration and the incident exercise were authored for
this task. The initial FleetServe implementation includes pilot-era semantics;
compatibility adapters and archived profiles retain their documented consumers.
The incident files are synthetic replay material, not a claim of a real outage.

The v6 numerical pilot extends adaptive batching with lifecycle/diagnostic methods
and adds fleetserve.inference. Its deployment contract intentionally exceeds the
original upstream batching assumptions. The fabric incident is captured from an
actual run of this synthetic pilot, with original per-process events retained.
Upstream copyrights and tests remain intact; root docs define the deployment scope.
