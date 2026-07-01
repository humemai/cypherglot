# Benchmark datasets

The runtime benchmark runs the *same* workload against two graph topologies so
that graph structure is isolated as an experimental factor:

- **`synthetic`** (default): a parametric generator with controlled scaling and
  uniform/skewed degree profiles. No external data; generated in-process.
- **`ldbc_snb`**: a real LDBC Social Network Benchmark graph (a skewed,
  message-dominated social network). Generated once with the official Datagen
  and loaded from disk.

## Generating LDBC SNB

```bash
scripts/benchmarks/data/generate_ldbc_snb.sh <SCALE_FACTOR> <OUTPUT_DIR> [PARALLELISM]
# e.g. the paper's two scale points:
scripts/benchmarks/data/generate_ldbc_snb.sh 1  /data/ldbc_snb/sf1
scripts/benchmarks/data/generate_ldbc_snb.sh 10 /data/ldbc_snb/sf10 8
```

This produces the Datagen `bi / csv / composite-merged-fk` layout. The loader
(`scripts/benchmarks/common/topology_ldbc_snb.py`) reads the
`initial_snapshot/{dynamic,static}` entities and maps them onto the corpus
contract:

| corpus slot   | LDBC SNB entity                         |
|---------------|-----------------------------------------|
| `node_type_1` | `Person`                                |
| `node_type_2` | `Message` = `Post` ∪ `Comment`          |
| `node_type_3` | `Tag`                                    |
| `edge_type_1` | `KNOWS` (`Person→Person`, bidirected)   |
| `edge_type_2` | `CREATED` (`Person→Message`)            |
| `edge_type_3` | `HAS_TAG` (`Message→Tag`)               |

The graph *structure* is verbatim SNB; only the analytic payload columns
(`score`, `active`, `weight`, `rank`, ...) are derived deterministically so the
schema width matches the synthetic topology and the workload is byte-identical.
64-bit SNB ids are remapped to compact per-type ordinals; `Person` ordinals are
assigned in descending `KNOWS`-degree order, so `person-000001` is the densest
hub. The first conversion is cached under the data dir
(`_cypherglot_fixture_v1/`) and reused across index modes, repeats, and engines.

## Running the benchmark on a topology

Single engine:

```bash
python -m scripts.benchmarks.runtime.sqlite \
  --topology ldbc_snb --ldbc-snb-data-dir /data/ldbc_snb/sf1 \
  --variable-hop-max 5 --index-mode both --output result.json
```

Full matrix (run once per topology; the workload is identical):

```bash
python -m scripts.benchmarks.runtime.matrix --scale large --topology synthetic ...
python -m scripts.benchmarks.runtime.matrix --scale large \
  --topology ldbc_snb --ldbc-snb-data-dir /data/ldbc_snb/sf10 ...
```
