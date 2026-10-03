# Inference observer starter, revision 3

This repository contains the initial implementation for a production LLM serving observation and recovery task. It exposes `python -m inference_observer --input <exports> --output <directory>` and uses the Python standard library.

The observer consumes gateway, scheduler, quality and deployment exports. Recovery now requires joint placement and barrier-wave execution with dependency, warmup resource, maintenance and cutover lease constraints. The prototype's static choices and dashboard timing preview require repair.

Runtime data is delivered by the isolated Harbor task. Complete contracts, machine-readable artifact fields and a public format checker are included here. Solutions, hidden evaluator code, expected results, run logs and credentials are not part of this public repository.
