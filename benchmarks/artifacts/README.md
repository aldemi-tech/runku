# Artifact regression benchmark

Run `make artifact-benchmark`. The benchmark hashes a deterministic 8 MiB blob, writes it durably,
and reads it repeatedly while checking size and digest.

Committed results are local regression baselines for the stated hardware and release profile. They
are not an SLO, SLA, or prediction of remote object-storage performance.

Record date, commit, profile, OS/architecture, CPU/memory/filesystem, warm/cold state, iterations,
raw output, and summary. Preserve durability/integrity checks while optimizing.
