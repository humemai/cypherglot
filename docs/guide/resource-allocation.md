# Resource allocation and comparability contract

What each engine is actually given during a runtime benchmark run, what is held
equal across engines, and what is not. Read this before quoting any
cross-engine number: several axes are **not** equalised, and the differences
are large enough to change conclusions.

Audited 2026-07-31 against the campaign fleet (Hetzner CCX33: 8 dedicated vCPU,
32 GiB RAM, local NVMe).

## Held equal

| Axis | Value | Mechanism |
|---|---|---|
| CPU budget | 4 of 8 vCPU per job | `--worker-cpusets`, applied to the client **and** its server containers |
| Co-runners | none at large scale (`--workers 1`) | one job per box; the dispatcher never co-runs two jobs of one backend |
| Dataset | identical fixture per scale | same generated graph or LDBC SNB directory |
| Query corpus | identical | same 14 OLTP / 16-19 OLAP queries, same order |
| Iterations / warmup | identical per scale | `--iterations`, `--warmup`, `--olap-iterations` |
| Query timeouts | identical | `--oltp-timeout-ms`, `--olap-timeout-ms`; a timeout is recorded as data |
| Job budget | identical | `--job-timeout-s` |

CPU *budget* is genuinely equal. What each engine does inside that budget is
not — see below.

## NOT held equal

### 1. Thread counts (CPU oversubscription)

Pinning restricts which cores a process may run on; it does not change what the
process *thinks* it has. Measured on a fleet box:

```
host cores                 : 8
process pinned to CPUs 0-3 :
  sched_getaffinity        : 4      <- pinning is in effect
  os.cpu_count()           : 8      <- but this is what engines size pools from
  duckdb default threads   : 8      <- 8 threads on 4 cores, 2:1 oversubscribed
```

No engine has an explicit thread cap set anywhere in the harness. Each uses its
own default, derived from total host cores rather than the affinity mask:

| Engine | Effective parallelism on a 4-core cpuset |
|---|---|
| DuckDB | 8 threads (2:1 oversubscribed) |
| ClickHouse | `max_threads` defaults to host cores, so also ~8 |
| PostgreSQL / AGE | `max_parallel_workers_per_gather` default (2) |
| Neo4j | its own pool sizing, host-core derived |
| ArcadeDB | JVM common pool, host-core derived |
| SQLite | single-threaded |
| LadybugDB | its own default |

So the honest description is **"each engine at its own default parallelism
within an equal 4-core budget"**, not "equal resources". Oversubscription
generally costs the oversubscriber, so this is unlikely to reverse a headline,
but it must not be described as equal.

### 2. Memory (the larger asymmetry)

**No container memory limit is set for any engine.** The harness passes
`--cpuset-cpus` to the server containers but never `--memory`. Consequently:

| Engine | Memory ceiling |
|---|---|
| ArcadeDB Embedded | **hard cap** — `-Xmx` per scale (4g / 16g / large clamped to 60% of host) |
| DuckDB, SQLite, LadybugDB, Turso | **none** — may use all 32 GiB |
| Neo4j, PostgreSQL, AGE, ClickHouse | library defaults, no cgroup limit |

ArcadeDB is the only engine running under an explicit ceiling. That is partly
inherent — a JVM must be given an `-Xmx`, native engines need no equivalent —
but the *value* is a harness choice, and this campaign showed it is decisive:
at 10M nodes ArcadeDB fails at every heap tried (too small and the JVM throws
`OutOfMemoryError`; too large and the kernel OOM-kills it), while DuckDB and
LadybugDB run unbounded on the same box.

Any statement of the form "engine X needs less memory than engine Y" is
therefore **not supported** by these runs. Peak RSS is a consequence of each
engine's own defaults, not of an imposed common budget.

### 3. Analytical structures

ArcadeDB is the only engine given a materialised analytical projection (a Graph
Analytical View) before the OLAP suite, with its build cost booked to setup as
`gav_ms` rather than to query latency. See the "OLAP execution paths are not
symmetric" section in `benchmarks.md`; the paired `--no-analytical-view` arm
exists to quantify it.

### 4. Vendor configuration

The July 2026 vendor-sizing block tuned memory settings for PostgreSQL, Neo4j
and AGE at synthetic-medium and snb-sf1 **only**. Every other engine, and every
engine at synthetic-large, runs at library defaults.

## Host layout requirements

These are not fairness issues but they decide whether a run completes at all;
see "Host provisioning requirements at the large tiers" in `benchmarks.md`.
`TMPDIR` and docker's `data-root` must be on the large volume, and the dataset
fixture must be budgeted separately from the databases.

## What to fix before the next campaign

1. **Set thread counts explicitly per engine** to match the cpuset (DuckDB
   `SET threads`, ClickHouse `max_threads`, PostgreSQL
   `max_parallel_workers_per_gather`). Cheap, and turns "each engine's default"
   into a genuine equal-parallelism claim.
2. **Set a container memory limit** (`--memory`) equal across server engines,
   and decide what the embedded engines' equivalent should be. Without this,
   memory-related claims are not comparable.
3. Until 1 and 2 are done, **state the asymmetries** rather than implying
   parity. An EA&B reviewer will accept a disclosed limitation; an undisclosed
   one that they find themselves is fatal.
