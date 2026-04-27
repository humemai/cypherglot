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
- `schema-small-r01-20260424T105439Z.json`
- `schema-small-r02-20260424T105439Z.json`
- `schema-small-r03-20260424T105439Z.json`

### Setup summary

| Schema | Connect | DDL | Ingest | Index | Analyze | RSS Connect | RSS DDL | RSS Ingest | RSS Index | RSS Analyze | Size | Pooled Mean | Pooled p50 | Pooled p95 | Pooled p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `45.93 ms +- 5.70` | `28.58 ms +- 1.21` | `186.85 ms +- 0.18` | `35.68 ms +- 0.34` | `4.29 ms +- 0.66` | `54.36 MiB +- 0.04` | `54.40 MiB +- 0.04` | `57.03 MiB +- 0.04` | `57.48 MiB +- 0.04` | `57.48 MiB +- 0.04` | `4.80 MiB +- 0.00` | `1.60 ms +- 0.00` | `0.06 ms +- 0.00` | `6.21 ms +- 0.01` | `7.38 ms +- 0.01` |
| typed-property | `34.19 ms +- 4.07` | `15.80 ms +- 5.45` | `384.60 ms +- 4.18` | `152.74 ms +- 5.18` | `38.77 ms +- 1.00` | `58.03 MiB +- 0.07` | `58.06 MiB +- 0.07` | `59.40 MiB +- 0.04` | `61.27 MiB +- 0.04` | `61.27 MiB +- 0.04` | `14.57 MiB +- 0.00` | `2.00 ms +- 0.00` | `0.91 ms +- 0.00` | `6.70 ms +- 0.01` | `7.98 ms +- 0.01` |
| type-aware | `53.80 ms +- 1.93` | `29.05 ms +- 0.85` | `80.67 ms +- 2.60` | `10.08 ms +- 0.56` | `2.92 ms +- 0.11` | `61.24 MiB +- 0.04` | `61.27 MiB +- 0.04` | `61.27 MiB +- 0.04` | `61.27 MiB +- 0.04` | `61.27 MiB +- 0.04` | `2.94 MiB +- 0.00` | `0.59 ms +- 0.00` | `0.03 ms +- 0.00` | `2.17 ms +- 0.02` | `2.50 ms +- 0.02` |

### Query mean summary


#### OLTP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.72 ms +- 0.00` | `0.01 ms +- 0.00` |

#### OLAP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.05 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.68 ms +- 0.01` | `8.31 ms +- 0.01` | `2.59 ms +- 0.02` |
| `relationship_stats` | `relationship-aggregate` | `1.79 ms +- 0.01` | `1.87 ms +- 0.01` | `0.90 ms +- 0.00` |

### Query p50 summary


#### OLTP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.71 ms +- 0.01` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.10 ms +- 0.00` | `0.09 ms +- 0.00` | `0.04 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `7.60 ms +- 0.01` | `8.27 ms +- 0.02` | `2.57 ms +- 0.03` |
| `relationship_stats` | `relationship-aggregate` | `1.78 ms +- 0.01` | `1.85 ms +- 0.00` | `0.89 ms +- 0.00` |

### Query p95 summary


#### OLTP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.82 ms +- 0.01` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.11 ms +- 0.00` | `0.10 ms +- 0.00` | `0.06 ms +- 0.01` |
| `relationship_projection` | `relationship-projection` | `8.22 ms +- 0.02` | `8.94 ms +- 0.02` | `2.70 ms +- 0.02` |
| `relationship_stats` | `relationship-aggregate` | `1.86 ms +- 0.01` | `1.94 ms +- 0.01` | `0.95 ms +- 0.00` |

### Query p99 summary


#### OLTP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.01 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `1.98 ms +- 0.03` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `0.14 ms +- 0.01` | `0.11 ms +- 0.01` | `0.07 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `8.73 ms +- 0.05` | `9.42 ms +- 0.02` | `3.01 ms +- 0.01` |
| `relationship_stats` | `relationship-aggregate` | `2.03 ms +- 0.04` | `2.11 ms +- 0.06` | `1.06 ms +- 0.02` |

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
- `schema-medium-r01-20260424T105858Z.json`
- `schema-medium-r02-20260424T105858Z.json`
- `schema-medium-r03-20260424T105858Z.json`

### Setup summary

| Schema | Connect | DDL | Ingest | Index | Analyze | RSS Connect | RSS DDL | RSS Ingest | RSS Index | RSS Analyze | Size | Pooled Mean | Pooled p50 | Pooled p95 | Pooled p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `38.82 ms +- 6.30` | `28.10 ms +- 0.59` | `36861.80 ms +- 193.78` | `9358.84 ms +- 183.17` | `961.38 ms +- 5.80` | `54.29 MiB +- 0.02` | `54.32 MiB +- 0.02` | `61.96 MiB +- 0.02` | `63.67 MiB +- 0.02` | `63.67 MiB +- 0.02` | `1160.55 MiB +- 0.00` | `228.40 ms +- 0.51` | `0.70 ms +- 0.00` | `917.22 ms +- 2.04` | `1103.41 ms +- 2.44` |
| typed-property | `39.58 ms +- 10.33` | `15.80 ms +- 5.35` | `100662.83 ms +- 3523.13` | `63860.92 ms +- 5379.44` | `9149.41 ms +- 37.54` | `69.73 MiB +- 0.07` | `69.76 MiB +- 0.07` | `77.53 MiB +- 0.02` | `77.53 MiB +- 0.02` | `77.53 MiB +- 0.02` | `3614.01 MiB +- 0.00` | `322.37 ms +- 0.91` | `98.96 ms +- 0.01` | `1158.45 ms +- 3.87` | `1390.69 ms +- 4.78` |
| type-aware | `37.63 ms +- 0.54` | `13.62 ms +- 5.55` | `19027.99 ms +- 254.37` | `3901.38 ms +- 100.92` | `690.51 ms +- 21.04` | `78.20 MiB +- 0.03` | `78.24 MiB +- 0.03` | `79.31 MiB +- 0.03` | `79.31 MiB +- 0.03` | `79.31 MiB +- 0.03` | `670.94 MiB +- 0.00` | `94.69 ms +- 0.04` | `0.26 ms +- 0.00` | `360.83 ms +- 0.29` | `422.46 ms +- 0.42` |

### Query mean summary


#### OLTP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `196.64 ms +- 0.02` | `0.01 ms +- 0.00` |

#### OLAP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.37 ms +- 0.00` | `1.28 ms +- 0.01` | `0.51 ms +- 0.01` |
| `relationship_projection` | `relationship-projection` | `1149.96 ms +- 2.54` | `1448.76 ms +- 5.00` | `437.87 ms +- 0.45` |
| `relationship_stats` | `relationship-aggregate` | `219.01 ms +- 0.55` | `287.54 ms +- 0.56` | `129.72 ms +- 0.21` |

### Query p50 summary


#### OLTP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `196.09 ms +- 0.08` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.37 ms +- 0.00` | `1.27 ms +- 0.01` | `0.50 ms +- 0.00` |
| `relationship_projection` | `relationship-projection` | `1148.62 ms +- 2.73` | `1447.82 ms +- 5.33` | `437.07 ms +- 0.43` |
| `relationship_stats` | `relationship-aggregate` | `218.29 ms +- 0.60` | `287.21 ms +- 0.58` | `128.95 ms +- 0.40` |

### Query p95 summary


#### OLTP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `199.61 ms +- 0.14` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.42 ms +- 0.00` | `1.31 ms +- 0.02` | `0.53 ms +- 0.04` |
| `relationship_projection` | `relationship-projection` | `1167.46 ms +- 1.70` | `1458.85 ms +- 5.08` | `446.27 ms +- 0.72` |
| `relationship_stats` | `relationship-aggregate` | `224.88 ms +- 0.22` | `290.04 ms +- 0.59` | `135.02 ms +- 0.56` |

### Query p99 summary


#### OLTP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.03 ms +- 0.00` | `201.25 ms +- 0.36` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `1.55 ms +- 0.03` | `1.35 ms +- 0.02` | `0.60 ms +- 0.08` |
| `relationship_projection` | `relationship-projection` | `1180.18 ms +- 1.17` | `1469.09 ms +- 3.54` | `453.16 ms +- 0.81` |
| `relationship_stats` | `relationship-aggregate` | `228.31 ms +- 0.31` | `292.45 ms +- 0.40` | `138.27 ms +- 0.55` |

## Large schema dataset

Runs: `3`

Controls:
- iterations: `500`
- warmup: `10`
- batch size: `10000`
- schemas: `json`, `typed`, `typeaware`

Dataset:
- node types: `10`
- edge types: `10`
- nodes per type: `1,000,000`
- edges per source: `8`
- multi-hop length: `8`
- total nodes: `10,000,000`
- total edges: `80,000,000`
- node properties per node: `text=2`, `numeric=10`, `boolean=2`
- edge properties per edge: `text=2`, `numeric=6`, `boolean=1`

Files:
- `schema-large-r01-20260424T131544Z.json`
- `schema-large-r02-20260424T131544Z.json`
- `schema-large-r03-20260424T131544Z.json`

### Setup summary

| Schema | Connect | DDL | Ingest | Index | Analyze | RSS Connect | RSS DDL | RSS Ingest | RSS Index | RSS Analyze | Size | Pooled Mean | Pooled p50 | Pooled p95 | Pooled p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| generic JSON | `50.36 ms +- 10.81` | `31.96 ms +- 6.30` | `1160595.42 ms +- 1437.40` | `259174.74 ms +- 5993.28` | `23210.79 ms +- 912.62` | `54.21 MiB +- 0.13` | `54.25 MiB +- 0.13` | `106.03 MiB +- 0.58` | `115.76 MiB +- 0.58` | `115.76 MiB +- 0.58` | `27521.76 MiB +- 0.00` | `10001.00 ms +- 4.41` | `1385.39 ms +- 9.43` | `30858.87 ms +- 10.56` | `32651.87 ms +- 13.19` |
| typed-property | `49.54 ms +- 1.59` | `18.88 ms +- 0.45` | `3784053.22 ms +- 3123.85` | `2683854.40 ms +- 46929.02` | `1551316.23 ms +- 18953.26` | `80.69 MiB +- 0.14` | `80.72 MiB +- 0.14` | `137.63 MiB +- 0.31` | `190.67 MiB +- 0.02` | `190.73 MiB +- 0.03` | `88688.90 MiB +- 0.00` | `10551.27 ms +- 102.39` | `3764.79 ms +- 4.95` | `30910.14 ms +- 184.40` | `33327.06 ms +- 90.22` |
| type-aware | `94.04 ms +- 43.25` | `17.09 ms +- 5.16` | `559374.62 ms +- 12745.86` | `113134.58 ms +- 23961.33` | `15941.25 ms +- 39.63` | `103.93 MiB +- 1.84` | `104.02 MiB +- 1.84` | `142.99 MiB +- 1.65` | `142.99 MiB +- 1.65` | `142.99 MiB +- 1.65` | `16050.43 MiB +- 0.00` | `3050.97 ms +- 14.46` | `885.31 ms +- 0.92` | `8881.86 ms +- 73.74` | `9373.28 ms +- 98.31` |

### Query mean summary


#### OLTP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `2217.85 ms +- 2.94` | `0.01 ms +- 0.00` |

#### OLAP-leaning query mean

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `33100.13 ms +- 13.86` | `21846.70 ms +- 538.32` | `7039.03 ms +- 18.63` |
| `relationship_projection` | `relationship-projection` | `24135.09 ms +- 2.60` | `33931.29 ms +- 66.77` | `9496.14 ms +- 104.46` |
| `relationship_stats` | `relationship-aggregate` | `2770.76 ms +- 18.86` | `5311.73 ms +- 10.37` | `1770.61 ms +- 1.84` |

### Query p50 summary


#### OLTP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.02 ms +- 0.00` | `2216.44 ms +- 2.29` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p50

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `32413.09 ms +- 34.78` | `21818.43 ms +- 534.45` | `6907.84 ms +- 1.82` |
| `relationship_projection` | `relationship-projection` | `24110.96 ms +- 1.00` | `33892.63 ms +- 45.58` | `9497.18 ms +- 119.91` |
| `relationship_stats` | `relationship-aggregate` | `2773.74 ms +- 23.82` | `5311.33 ms +- 11.31` | `1769.62 ms +- 1.90` |

### Query p95 summary


#### OLTP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.02 ms +- 0.00` | `0.02 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.00 ms +- 0.00` | `0.01 ms +- 0.00` | `0.00 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.03 ms +- 0.00` | `2234.77 ms +- 3.86` | `0.01 ms +- 0.00` |

#### OLAP-leaning query p95

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `38879.08 ms +- 575.31` | `22111.61 ms +- 528.27` | `7533.53 ms +- 24.13` |
| `relationship_projection` | `relationship-projection` | `24278.50 ms +- 21.33` | `34135.58 ms +- 75.47` | `9568.05 ms +- 126.01` |
| `relationship_stats` | `relationship-aggregate` | `2797.08 ms +- 32.97` | `5337.62 ms +- 14.36` | `1785.48 ms +- 3.53` |

### Query p99 summary


#### OLTP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `one_hop_neighbors` | `adjacency-read` | `0.03 ms +- 0.00` | `0.03 ms +- 0.00` | `0.01 ms +- 0.00` |
| `point_lookup` | `point-read` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` | `0.01 ms +- 0.00` |
| `top_active_score` | `ordered-top-k` | `0.03 ms +- 0.00` | `2244.53 ms +- 2.77` | `0.02 ms +- 0.00` |

#### OLAP-leaning query p99

| Query | Category | generic JSON | typed-property | type-aware |
| --- | --- | ---: | ---: | ---: |
| `multi_hop_chain` | `multi-hop-read` | `42116.65 ms +- 694.39` | `22174.27 ms +- 537.58` | `8067.57 ms +- 254.55` |
| `relationship_projection` | `relationship-projection` | `24785.91 ms +- 11.53` | `34501.84 ms +- 103.62` | `9787.68 ms +- 195.17` |
| `relationship_stats` | `relationship-aggregate` | `2804.21 ms +- 32.64` | `5352.42 ms +- 12.56` | `1795.11 ms +- 2.40` |
