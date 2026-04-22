# Schema benchmark repeated-run summary

Inputs: `scripts/benchmarks/results/schema`

## Small schema dataset

Runs: `3`

Controls:
- iterations: `10000`
- warmup: `200`
- batch size: `1000`
- schemas: `json`, `typed`, `typeaware`

Files:
- `schema-small-r01-20260422T092228Z.json`
- `schema-small-r02-20260422T092228Z.json`
- `schema-small-r03-20260422T092228Z.json`

### Setup summary

| Schema | Connect | DDL | Ingest | Index | Analyze | RSS Connect | RSS DDL | RSS Ingest | RSS Index | RSS Analyze | Size | Pooled Mean | Pooled p50 | Pooled p95 | Pooled p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `47.33 ms +- 10.60` | `30.45 ms +- 1.46` | `191.04 ms +- 5.35` | `35.47 ms +- 0.85` | `3.71 ms +- 0.04` | `54.29 MiB +- 0.02` | `54.33 MiB +- 0.02` | `56.92 MiB +- 0.02` | `57.38 MiB +- 0.02` | `57.38 MiB +- 0.02` | `4.80 MiB +- 0.00` | `1.54 ms +- 0.00` | `0.06 ms +- 0.00` | `5.94 ms +- 0.02` | `7.05 ms +- 0.02` |
| typed-property | `34.01 ms +- 7.03` | `19.82 ms +- 11.09` | `353.36 ms +- 2.31` | `149.87 ms +- 9.51` | `37.60 ms +- 0.43` | `57.75 MiB +- 0.02` | `57.78 MiB +- 0.02` | `59.27 MiB +- 0.02` | `61.15 MiB +- 0.02` | `61.15 MiB +- 0.02` | `14.57 MiB +- 0.00` | `1.91 ms +- 0.00` | `0.90 ms +- 0.01` | `6.29 ms +- 0.01` | `7.48 ms +- 0.01` |
| type-aware | `26.15 ms +- 0.34` | `9.63 ms +- 0.15` | `74.56 ms +- 1.99` | `9.32 ms +- 0.08` | `2.82 ms +- 0.01` | `61.12 MiB +- 0.02` | `61.15 MiB +- 0.02` | `61.15 MiB +- 0.02` | `61.15 MiB +- 0.02` | `61.15 MiB +- 0.02` | `2.94 MiB +- 0.00` | `0.58 ms +- 0.00` | `0.03 ms +- 0.00` | `2.11 ms +- 0.00` | `2.43 ms +- 0.00` |

### Query mean summary


#### OLTP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.72 ms +- 0.01` | `0.01 ms +- 0.00` |

#### OLAP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.04 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.33 ms +- 0.02` | `7.78 ms +- 0.01` | `2.52 ms +- 0.00` |
| `relationship_stats` | `relationship-aggregate` | `1.77 ms +- 0.00` | `1.85 ms +- 0.02` | `0.90 ms +- 0.02` |

### Query p50 summary


#### OLTP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.72 ms +- 0.01` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.04 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.32 ms +- 0.02` | `7.77 ms +- 0.01` | `2.52 ms +- 0.00` |
| `relationship_stats` | `relationship-aggregate` | `1.77 ms +- 0.01` | `1.85 ms +- 0.02` | `0.90 ms +- 0.02` |

### Query p95 summary


#### OLTP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.74 ms +- 0.02` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.05 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.40 ms +- 0.02` | `7.83 ms +- 0.02` | `2.56 ms +- 0.00` |
| `relationship_stats` | `relationship-aggregate` | `1.79 ms +- 0.01` | `1.86 ms +- 0.02` | `0.91 ms +- 0.01` |

### Query p99 summary


#### OLTP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.75 ms +- 0.02` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.11 ms +- 0.01` | `0.10 ms +- 0.00` | `0.05 ms +- 0.01` |
| `relationship_projection` | `relationship-projection` | `7.45 ms +- 0.01` | `7.92 ms +- 0.02` | `2.58 ms +- 0.00` |
| `relationship_stats` | `relationship-aggregate` | `1.80 ms +- 0.00` | `1.87 ms +- 0.02` | `0.92 ms +- 0.01` |

## Medium schema dataset

Runs: `3`

Controls:
- iterations: `5000`
- warmup: `100`
- batch size: `5000`
- schemas: `json`, `typed`, `typeaware`

Files:
- `schema-medium-r01-20260422T092742Z.json`
- `schema-medium-r02-20260422T092742Z.json`
- `schema-medium-r03-20260422T092742Z.json`

### Setup summary

| Schema | Connect | DDL | Ingest | Index | Analyze | RSS Connect | RSS DDL | RSS Ingest | RSS Index | RSS Analyze | Size | Pooled Mean | Pooled p50 | Pooled p95 | Pooled p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `35.19 ms +- 0.03` | `12.02 ms +- 5.08` | `1859.88 ms +- 5.12` | `320.38 ms +- 0.28` | `46.40 ms +- 0.44` | `54.02 MiB +- 0.10` | `54.05 MiB +- 0.10` | `59.35 MiB +- 0.11` | `61.28 MiB +- 0.11` | `61.28 MiB +- 0.11` | `55.39 MiB +- 0.00` | `10.54 ms +- 0.29` | `0.66 ms +- 0.00` | `41.48 ms +- 1.30` | `49.90 ms +- 1.64` |
| typed-property | `42.79 ms +- 13.64` | `22.33 ms +- 10.93` | `4374.14 ms +- 82.68` | `1963.62 ms +- 18.18` | `458.34 ms +- 1.08` | `62.48 MiB +- 1.83` | `62.51 MiB +- 1.83` | `68.39 MiB +- 0.60` | `69.31 MiB +- 0.16` | `69.31 MiB +- 0.16` | `174.40 MiB +- 0.00` | `14.30 ms +- 0.01` | `5.03 ms +- 0.00` | `50.56 ms +- 0.02` | `60.72 ms +- 0.03` |
| type-aware | `30.48 ms +- 5.30` | `13.09 ms +- 5.24` | `950.11 ms +- 7.00` | `158.70 ms +- 3.19` | `32.31 ms +- 0.16` | `65.38 MiB +- 2.24` | `65.42 MiB +- 2.24` | `65.61 MiB +- 1.97` | `65.81 MiB +- 1.63` | `65.81 MiB +- 1.63` | `33.00 MiB +- 0.00` | `3.97 ms +- 0.00` | `0.24 ms +- 0.01` | `14.80 ms +- 0.02` | `17.30 ms +- 0.03` |

### Query mean summary


#### OLTP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `8.91 ms +- 0.01` | `0.01 ms +- 0.00` |

#### OLAP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.30 ms +- 0.00` | `1.16 ms +- 0.00` | `0.47 ms +- 0.02` |
| `relationship_projection` | `relationship-projection` | `52.01 ms +- 1.73` | `63.25 ms +- 0.03` | `17.93 ms +- 0.03` |
| `relationship_stats` | `relationship-aggregate` | `9.89 ms +- 0.02` | `12.49 ms +- 0.05` | `5.42 ms +- 0.03` |

### Query p50 summary


#### OLTP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `8.89 ms +- 0.01` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.30 ms +- 0.00` | `1.16 ms +- 0.00` | `0.47 ms +- 0.02` |
| `relationship_projection` | `relationship-projection` | `51.97 ms +- 1.72` | `63.20 ms +- 0.02` | `17.93 ms +- 0.02` |
| `relationship_stats` | `relationship-aggregate` | `9.87 ms +- 0.02` | `12.47 ms +- 0.05` | `5.42 ms +- 0.03` |

### Query p95 summary


#### OLTP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `9.01 ms +- 0.01` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.34 ms +- 0.02` | `1.18 ms +- 0.01` | `0.47 ms +- 0.02` |
| `relationship_projection` | `relationship-projection` | `52.43 ms +- 1.71` | `63.77 ms +- 0.08` | `18.33 ms +- 0.03` |
| `relationship_stats` | `relationship-aggregate` | `10.05 ms +- 0.01` | `12.55 ms +- 0.06` | `5.47 ms +- 0.03` |

### Query p99 summary


#### OLTP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `9.23 ms +- 0.05` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.51 ms +- 0.04` | `1.19 ms +- 0.01` | `0.48 ms +- 0.02` |
| `relationship_projection` | `relationship-projection` | `52.92 ms +- 1.82` | `64.54 ms +- 0.23` | `18.55 ms +- 0.03` |
| `relationship_stats` | `relationship-aggregate` | `10.30 ms +- 0.02` | `12.89 ms +- 0.28` | `5.51 ms +- 0.04` |
