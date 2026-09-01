# Release repository regression benchmark

Run `make release-repository-benchmark`. The benchmark exercises the durable Release repository and
records a local performance baseline.

Results detect repeated regressions on comparable hardware. They do not define remote PostgreSQL
capacity or a production SLO.

Record adapter, dataset, operation mix, concurrency, commit, hardware, and raw output. Never weaken
lifecycle, compatibility, atomicity, or compare-and-set semantics for a faster number.
