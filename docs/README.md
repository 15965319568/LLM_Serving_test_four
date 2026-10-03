# Inference observer starter, revision 2

This repository contains the initial implementation for a production LLM serving observation and recovery task. It exposes `python -m inference_observer --input <exports> --output <directory>` and uses the Python standard library.

The observer consumes gateway, scheduler, quality and deployment exports. The recovery module produces control evidence, placement options, assignments and scenario decisions. Its prototype behavior needs repair against the task's runtime business contracts.

Runtime data and contracts are delivered by the isolated Harbor task. Solutions, evaluator code, expected results, run logs and credentials are not part of this public repository.
