# Harbor submission environment — V7.1

V7.1 corrects the author task package to use an independent verifier container.
The public V7 task source remains pinned to
`9b97694ef07b12f2d4077f1b8a4424cd95f42c8b`.

The task package explicitly sets `environment_mode = "separate"` under
`[verifier]`, supplies `tests/Dockerfile` and a verifier-specific resource
configuration, and declares `/workspace` as an artifact. Harbor transfers the
completed project into the fresh verifier container before running grading.
New source files are transferred as well. The verifier has independently
installed, locked dependencies and receives no agent-installed environment.

The private task archive, reference solution and grading files are distributed
to the submission platform separately. This repository contains the public
starter project. The V7.1 packaging change does not alter the task requirements
or behavior checks.

Existing V7 model evaluation evidence was produced with the earlier shared
verifier package. It must retain that version label; packaging validation is
not a new model evaluation result.
