# Schema benchmark repeated-run summary

Inputs: `scripts/benchmarks/results/schema`

## Small schema dataset

Runs: `3`

Controls:
- iterations: `10000`
- warmup: `200`
- batch size: `1000`
- schemas: `json`, `typed`, `typeaware`

Dataset:
- node types: `4`
- edge types: `4`
- nodes per type: `1,000`
- edges per source: `3`
- multi-hop length: `4`
- total nodes: `4,000`
- total edges: `12,000`
- node properties per node: `text=2`, `numeric=10`, `boolean=2`
- edge properties per edge: `text=2`, `numeric=6`, `boolean=1`

Files:
- `schema-small-r01-20260422T212315Z.json`
- `schema-small-r02-20260422T212315Z.json`
- `schema-small-r03-20260422T212315Z.json`

### Setup summary

| Schema | Connect | DDL | Ingest | Index | Analyze | RSS Connect | RSS DDL | RSS Ingest | RSS Index | RSS Analyze | Size | Pooled Mean | Pooled p50 | Pooled p95 | Pooled p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `58.26 ms +- 4.33` | `27.88 ms +- 0.83` | `199.00 ms +- 4.92` | `35.07 ms +- 0.01` | `4.05 ms +- 0.03` | `54.38 MiB +- 0.02` | `54.41 MiB +- 0.02` | `57.02 MiB +- 0.02` | `57.48 MiB +- 0.02` | `57.48 MiB +- 0.02` | `4.80 MiB +- 0.00` | `1.55 ms +- 0.00` | `0.06 ms +- 0.00` | `5.98 ms +- 0.02` | `7.10 ms +- 0.02` |
| typed-property | `32.05 ms +- 5.19` | `15.54 ms +- 5.56` | `351.85 ms +- 2.11` | `151.21 ms +- 6.21` | `38.48 ms +- 1.53` | `58.01 MiB +- 0.02` | `58.04 MiB +- 0.02` | `59.38 MiB +- 0.02` | `61.26 MiB +- 0.02` | `61.26 MiB +- 0.02` | `14.57 MiB +- 0.00` | `1.91 ms +- 0.00` | `0.89 ms +- 0.01` | `6.31 ms +- 0.01` | `7.50 ms +- 0.01` |
| type-aware | `48.45 ms +- 17.97` | `24.71 ms +- 13.09` | `74.99 ms +- 1.29` | `9.15 ms +- 0.04` | `2.75 ms +- 0.02` | `61.23 MiB +- 0.02` | `61.26 MiB +- 0.02` | `61.26 MiB +- 0.02` | `61.26 MiB +- 0.02` | `61.26 MiB +- 0.02` | `2.94 MiB +- 0.00` | `0.58 ms +- 0.00` | `0.03 ms +- 0.00` | `2.12 ms +- 0.01` | `2.44 ms +- 0.01` |

### Query mean summary


#### OLTP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.70 ms +- 0.03` | `0.01 ms +- 0.00` |

#### OLAP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.04 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.38 ms +- 0.02` | `7.80 ms +- 0.01` | `2.52 ms +- 0.01` |
| `relationship_stats` | `relationship-aggregate` | `1.78 ms +- 0.00` | `1.84 ms +- 0.01` | `0.89 ms +- 0.00` |

### Query p50 summary


#### OLTP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.69 ms +- 0.03` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.04 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.34 ms +- 0.02` | `7.79 ms +- 0.00` | `2.52 ms +- 0.02` |
| `relationship_stats` | `relationship-aggregate` | `1.77 ms +- 0.00` | `1.84 ms +- 0.01` | `0.89 ms +- 0.01` |

### Query p95 summary


#### OLTP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.72 ms +- 0.02` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.05 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.64 ms +- 0.03` | `7.87 ms +- 0.02` | `2.57 ms +- 0.02` |
| `relationship_stats` | `relationship-aggregate` | `1.85 ms +- 0.01` | `1.86 ms +- 0.02` | `0.90 ms +- 0.00` |

### Query p99 summary


#### OLTP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.76 ms +- 0.02` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.12 ms +- 0.02` | `0.10 ms +- 0.00` | `0.05 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `8.01 ms +- 0.03` | `8.05 ms +- 0.05` | `2.60 ms +- 0.02` |
| `relationship_stats` | `relationship-aggregate` | `1.98 ms +- 0.03` | `1.87 ms +- 0.02` | `0.91 ms +- 0.01` |

## Medium schema dataset

Runs: `3`

Controls:
- iterations: `2000`
- warmup: `50`
- batch size: `5000`
- schemas: `json`, `typed`, `typeaware`

Dataset:
- node types: `6`
- edge types: `8`
- nodes per type: `100,000`
- edges per source: `4`
- multi-hop length: `5`
- total nodes: `600,000`
- total edges: `3,200,000`
- node properties per node: `text=2`, `numeric=10`, `boolean=2`
- edge properties per edge: `text=2`, `numeric=6`, `boolean=1`

Files:
- `schema-medium-r01-20260422T212723Z.json`
- `schema-medium-r02-20260422T212723Z.json`
- `schema-medium-r03-20260422T212723Z.json`

### Setup summary

| Schema | Connect | DDL | Ingest | Index | Analyze | RSS Connect | RSS DDL | RSS Ingest | RSS Index | RSS Analyze | Size | Pooled Mean | Pooled p50 | Pooled p95 | Pooled p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `53.91 ms +- 9.98` | `31.58 ms +- 6.33` | `34964.09 ms +- 89.64` | `7374.50 ms +- 92.22` | `949.19 ms +- 5.09` | `54.27 MiB +- 0.11` | `54.31 MiB +- 0.11` | `61.90 MiB +- 0.12` | `63.61 MiB +- 0.12` | `63.61 MiB +- 0.12` | `1160.55 MiB +- 0.00` | `224.45 ms +- 0.11` | `0.69 ms +- 0.00` | `902.38 ms +- 0.42` | `1086.15 ms +- 0.50` |
| typed-property | `45.04 ms +- 0.45` | `15.42 ms +- 5.06` | `98632.86 ms +- 912.78` | `54035.21 ms +- 593.08` | `9174.89 ms +- 205.46` | `69.72 MiB +- 0.09` | `69.75 MiB +- 0.09` | `77.84 MiB +- 0.75` | `77.84 MiB +- 0.75` | `77.84 MiB +- 0.75` | `3614.01 MiB +- 0.00` | `320.07 ms +- 0.24` | `100.40 ms +- 0.11` | `1146.60 ms +- 0.99` | `1376.04 ms +- 1.24` |
| type-aware | `38.29 ms +- 4.05` | `15.17 ms +- 5.18` | `18683.28 ms +- 184.06` | `4026.89 ms +- 243.36` | `679.83 ms +- 14.04` | `78.10 MiB +- 1.68` | `78.13 MiB +- 1.68` | `79.28 MiB +- 1.57` | `79.28 MiB +- 1.57` | `79.28 MiB +- 1.57` | `670.94 MiB +- 0.00` | `91.65 ms +- 0.25` | `0.26 ms +- 0.00` | `349.07 ms +- 0.93` | `408.58 ms +- 1.07` |

### Query mean summary


#### OLTP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `199.53 ms +- 0.24` | `0.01 ms +- 0.00` |

#### OLAP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.37 ms +- 0.00` | `1.27 ms +- 0.01` | `0.50 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `1132.09 ms +- 0.52` | `1433.40 ms +- 1.30` | `423.46 ms +- 1.10` |
| `relationship_stats` | `relationship-aggregate` | `213.23 ms +- 0.20` | `286.20 ms +- 0.22` | `125.89 ms +- 0.44` |

### Query p50 summary


#### OLTP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `199.23 ms +- 0.35` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.37 ms +- 0.00` | `1.27 ms +- 0.01` | `0.50 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `1131.72 ms +- 0.62` | `1432.90 ms +- 1.02` | `423.32 ms +- 1.10` |
| `relationship_stats` | `relationship-aggregate` | `213.12 ms +- 0.23` | `285.90 ms +- 0.17` | `125.83 ms +- 0.45` |

### Query p95 summary


#### OLTP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `201.91 ms +- 0.72` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.39 ms +- 0.00` | `1.28 ms +- 0.01` | `0.51 ms +- 0.01` |
| `relationship_projection` | `relationship-projection` | `1138.44 ms +- 0.55` | `1440.30 ms +- 2.42` | `427.31 ms +- 1.63` |
| `relationship_stats` | `relationship-aggregate` | `214.49 ms +- 0.09` | `288.06 ms +- 0.46` | `126.79 ms +- 0.43` |

### Query p99 summary


#### OLTP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.03 ms +- 0.00` | `203.55 ms +- 0.43` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.43 ms +- 0.05` | `1.29 ms +- 0.02` | `0.54 ms +- 0.03` |
| `relationship_projection` | `relationship-projection` | `1142.47 ms +- 0.52` | `1444.75 ms +- 2.17` | `429.96 ms +- 1.34` |
| `relationship_stats` | `relationship-aggregate` | `216.58 ms +- 0.34` | `291.78 ms +- 0.97` | `127.90 ms +- 0.47` |
