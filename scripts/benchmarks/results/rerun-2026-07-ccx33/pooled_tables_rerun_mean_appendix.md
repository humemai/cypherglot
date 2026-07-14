
## Engine x workload (pooled execute p50 ms, mean±std over repeats)

### ladybug-0181/snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 64.84±10.08 | - | 354.52±46.61 |

### ladybug-0181/synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 8.70±2.16 | - | 2575.11±98.50 |

### ladybug-0181/synthetic-small

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 0.94±0.23 | - | 5.21±0.56 |

### ladybug-medium/ladybug-snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 34.54±3.15 | - | 274.67±31.92 |

### ladybug-medium/ladybug-synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 7.77±2.25 | - | 2526.69±114.79 |

### large/snb-sf10

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| clickhouse | 749.60±77.82 | - | 3184.02±148.61 | - |
| neo4j | - | 3190.98 | - | 27471.50 |

### large/synthetic-large

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| postgresql | 0.81±0.03 | 582.63±73.78 | 1335.77±43.70 | 2937.01±202.76 |
| duckdb | 3.99±0.29 | 8.54±0.57 | 6073.08±1186.86 | 5811.42±1191.66 |
| clickhouse | 78.06±20.12 | - | 6486.84±569.36 | - |
| sqlite | 0.12±0.00 | 868.21±114.46 | 6594.80±713.91 | 6345.20±3426.64 |
| neo4j | 1.15±0.08 | 1469.69±121.84 | 9129.59±387.21 | 9972.12±413.98 |
| age | 295.47±67.38 | 841.78±132.09 | 12891.00±604.58 | 12961.04±688.53 |

### medium/snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| arcadedb-embedded | 0.14±0.01 | 250.19±6.92 | 162.64±8.50 | 163.76±6.77 |
| neo4j | 3.15±0.07 | 160.86±33.60 | 3079.15±249.87 | 2889.45±185.26 |
| duckdb | 20.85±1.51 | 6.16±0.52 | 3340.68±5589.77 | 6060.21±7357.47 |
| age | 11.52±13.38 | 36.58±16.88 | 3442.44±202.21 | 3401.52±199.86 |
| clickhouse | 110.92±22.87 | - | 3810.30±1727.13 | - |
| sqlite | 20.79±1.28 | 132.04±13.31 | 4106.09±384.45 | 1453.52±98.46 |
| turso | 31.63±1.60 | 1688.81±124.35 | 6429.88±623.19 | 4750.26±264.05 |
| postgresql | 9.27±0.31 | 120.15±11.99 | 10488.57±1189.66 | 12303.64±1314.33 |

### medium/synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| duckdb | 1.95±0.24 | 3.23±0.45 | 478.65±41.09 | 483.29±45.03 |
| clickhouse | 9.38±1.30 | - | 539.71±49.40 | - |
| age | 8.05±0.53 | 31.58±3.59 | 902.37±64.31 | 902.23±52.58 |
| arcadedb-embedded | 0.24±0.02 | 59.63±1.26 | 1686.69±198.25 | 1665.91±223.99 |
| turso | 0.28±0.02 | 307.27±18.01 | 2198.41±118.88 | 3658.11±200.13 |
| postgresql | 0.74±0.16 | 57.05±4.51 | 4918.89±411.28 | 5009.64±506.07 |
| neo4j | 0.94±0.08 | 84.66±5.31 | 13892.53±572.33 | 14616.87±650.00 |
| sqlite | 0.10±0.00 | 70.14±8.36 | 15896.20±5046.61 | 13737.48±6986.57 |

### small/synthetic-small

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| sqlite | 0.05±0.00 | 0.14±0.01 | 1.36±0.06 | 1.54±0.10 |
| postgresql | 0.68±0.16 | 0.86±0.17 | 2.01±0.28 | 2.07±0.29 |
| duckdb | 1.32±0.13 | 1.20±0.10 | 2.40±0.32 | 2.22±0.34 |
| turso | 0.33±0.01 | 1.27±0.05 | 3.34±0.19 | 4.80±0.20 |
| neo4j | 1.14±0.11 | 1.32±0.10 | 3.71±0.40 | 3.64±0.37 |
| arcadedb-embedded | 0.19±0.01 | 1.27±0.07 | 4.41±0.24 | 4.12±0.21 |
| ladybug | - | 1.38±0.46 | - | 5.45±0.98 |
| clickhouse | 3.53±1.05 | - | 6.94±1.47 | - |
| age | 0.55±0.09 | 0.73±0.18 | 73.55±6.03 | 74.11±5.74 |

### vendor-sizing/snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| neo4j | 3.30±0.21 | 118.70±3.53 | 1941.12±75.56 | 1787.25±76.29 |
| age | 20.00±15.92 | 15.94±11.43 | 3414.88±170.13 | 2697.95±144.72 |
| postgresql | 9.00±0.45 | 103.59±8.91 | 10176.21±1222.18 | 10503.85±1238.06 |

### vendor-sizing/synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| age | 9.06±0.39 | 32.33±2.56 | 1013.73±69.42 | 1004.45±31.52 |
| postgresql | 0.73±0.18 | 63.12±2.21 | 4873.99±390.26 | 4840.53±444.53 |
| neo4j | 1.02±0.19 | 85.05±5.17 | 13024.41±620.47 | 13543.45±637.54 |


## Index-removal sensitivity (unindexed / indexed, execute p50)

| dataset | engine | OLTP ratio | OLAP ratio |
|---|---|---|---|
| large/synthetic-large | age | 2.8x | 1.0x |
| large/synthetic-large | duckdb | 2.1x | 1.0x |
| large/synthetic-large | neo4j | 1278.8x | 1.1x |
| large/synthetic-large | postgresql | 717.6x | 2.2x |
| large/synthetic-large | sqlite | 7021.5x | 1.0x |
| medium/snb-sf1 | age | 3.2x | 1.0x |
| medium/snb-sf1 | arcadedb-embedded | 1842.7x | 1.0x |
| medium/snb-sf1 | duckdb | 0.3x | 1.8x |
| medium/snb-sf1 | neo4j | 51.1x | 0.9x |
| medium/snb-sf1 | postgresql | 13.0x | 1.2x |
| medium/snb-sf1 | sqlite | 6.4x | 0.4x |
| medium/snb-sf1 | turso | 53.4x | 0.7x |
| medium/synthetic-medium | age | 3.9x | 1.0x |
| medium/synthetic-medium | arcadedb-embedded | 245.3x | 1.0x |
| medium/synthetic-medium | duckdb | 1.7x | 1.0x |
| medium/synthetic-medium | neo4j | 89.8x | 1.1x |
| medium/synthetic-medium | postgresql | 77.4x | 1.0x |
| medium/synthetic-medium | sqlite | 673.7x | 0.9x |
| medium/synthetic-medium | turso | 1104.5x | 1.7x |
| small/synthetic-small | age | 1.3x | 1.0x |
| small/synthetic-small | arcadedb-embedded | 6.5x | 0.9x |
| small/synthetic-small | duckdb | 0.9x | 0.9x |
| small/synthetic-small | neo4j | 1.2x | 1.0x |
| small/synthetic-small | postgresql | 1.3x | 1.0x |
| small/synthetic-small | sqlite | 2.8x | 1.1x |
| small/synthetic-small | turso | 3.8x | 1.4x |
| vendor-sizing/snb-sf1 | age | 0.8x | 0.8x |
| vendor-sizing/snb-sf1 | neo4j | 36.0x | 0.9x |
| vendor-sizing/snb-sf1 | postgresql | 11.5x | 1.0x |
| vendor-sizing/synthetic-medium | age | 3.6x | 1.0x |
| vendor-sizing/synthetic-medium | neo4j | 83.2x | 1.0x |
| vendor-sizing/synthetic-medium | postgresql | 86.5x | 1.0x |


## Variable-length lowering: unroll vs recursive CTE (execute p50 ms)

| dataset | engine | mode | query | unroll | rcte | rcte/unroll |
|---|---|---|---|---|---|---|
| large/synthetic-large | clickhouse | indexed | olap_variable_length_grouped_max_rollup | 6494.75±344.34 | 3213.56±414.89 | 0.49x |
| large/synthetic-large | clickhouse | indexed | olap_variable_length_grouped_rollup | 29856.06±2469.12 | 58993.49±6080.13 | 1.98x |
| large/synthetic-large | clickhouse | indexed | olap_variable_length_reachability | 6438.89±327.37 | 3210.66±378.49 | 0.50x |
| large/synthetic-large | duckdb | indexed | olap_variable_length_grouped_max_rollup | 674.01±31.76 | 44.85±5.49 | 0.07x |
| large/synthetic-large | duckdb | indexed | olap_variable_length_grouped_rollup | 11570.02±719.34 | 97580.80±21617.74 | 8.43x |
| large/synthetic-large | duckdb | indexed | olap_variable_length_reachability | 831.62±43.07 | 50.50±1.89 | 0.06x |
| large/synthetic-large | duckdb | unindexed | olap_variable_length_grouped_max_rollup | 671.19±33.87 | 52.88±7.23 | 0.08x |
| large/synthetic-large | duckdb | unindexed | olap_variable_length_grouped_rollup | 11572.58±691.84 | 92587.28±21664.83 | 8.00x |
| large/synthetic-large | duckdb | unindexed | olap_variable_length_reachability | 831.01±48.50 | 54.53±5.42 | 0.07x |
| large/synthetic-large | sqlite | unindexed | olap_variable_length_grouped_max_rollup | 70245.13 | 2523.09 | 0.04x |
| large/synthetic-large | sqlite | unindexed | olap_variable_length_reachability | 79145.04 | 2620.85±212.14 | 0.03x |
| medium/snb-sf1 | duckdb | indexed | olap_variable_length_grouped_rollup | 29241.15±2411.19 | 183455.93 | 6.27x |
| medium/snb-sf1 | duckdb | unindexed | olap_variable_length_grouped_rollup | 28635.07±3229.83 | 195363.30±16669.75 | 6.82x |
| medium/synthetic-medium | clickhouse | indexed | olap_variable_length_grouped_max_rollup | 132.82±10.82 | 111.67±34.98 | 0.84x |
| medium/synthetic-medium | clickhouse | indexed | olap_variable_length_grouped_rollup | 2886.33±226.88 | 5673.52±563.99 | 1.97x |
| medium/synthetic-medium | clickhouse | indexed | olap_variable_length_reachability | 135.43±7.85 | 108.81±29.10 | 0.80x |
| medium/synthetic-medium | duckdb | indexed | olap_variable_length_grouped_max_rollup | 19.51±2.21 | 8.54±0.80 | 0.44x |
| medium/synthetic-medium | duckdb | indexed | olap_variable_length_grouped_rollup | 1306.36±65.08 | 7138.51±672.63 | 5.46x |
| medium/synthetic-medium | duckdb | indexed | olap_variable_length_reachability | 24.34±4.50 | 11.04±1.34 | 0.45x |
| medium/synthetic-medium | duckdb | unindexed | olap_variable_length_grouped_max_rollup | 21.77±2.04 | 10.57±0.96 | 0.49x |
| medium/synthetic-medium | duckdb | unindexed | olap_variable_length_grouped_rollup | 1299.58±64.97 | 7224.63±749.96 | 5.56x |
| medium/synthetic-medium | duckdb | unindexed | olap_variable_length_reachability | 24.65±3.39 | 11.96±1.36 | 0.49x |
| medium/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_max_rollup | 40.71±1.60 | 0.99±0.25 | 0.02x |
| medium/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_rollup | 10327.10±1147.10 | 81363.75±6501.98 | 7.88x |
| medium/synthetic-medium | postgresql | indexed | olap_variable_length_reachability | 40.16±1.36 | 0.91±0.25 | 0.02x |
| medium/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_max_rollup | 533.47±69.13 | 373.92±59.83 | 0.70x |
| medium/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_rollup | 10428.26±1165.50 | 79839.90±7914.50 | 7.66x |
| medium/synthetic-medium | postgresql | unindexed | olap_variable_length_reachability | 555.74±80.61 | 382.93±61.17 | 0.69x |
| medium/synthetic-medium | sqlite | indexed | olap_variable_length_grouped_max_rollup | 0.08±0.01 | 2.79±0.35 | 35.77x |
| medium/synthetic-medium | sqlite | indexed | olap_variable_length_grouped_rollup | 82976.29±6265.64 | 262642.19±3584.05 | 3.17x |
| medium/synthetic-medium | sqlite | indexed | olap_variable_length_reachability | 0.11±0.01 | 4.35±0.59 | 38.90x |
| medium/synthetic-medium | sqlite | unindexed | olap_variable_length_grouped_max_rollup | 2366.91±186.97 | 219.27±16.65 | 0.09x |
| medium/synthetic-medium | sqlite | unindexed | olap_variable_length_grouped_rollup | 77491.34±5922.64 | 285235.99±2458.72 | 3.68x |
| medium/synthetic-medium | sqlite | unindexed | olap_variable_length_reachability | 2800.92±201.01 | 217.76±16.38 | 0.08x |
| small/synthetic-small | clickhouse | indexed | olap_variable_length_grouped_max_rollup | 12.85±3.51 | 10.58±2.94 | 0.82x |
| small/synthetic-small | clickhouse | indexed | olap_variable_length_grouped_rollup | 10.89±3.46 | 11.40±3.33 | 1.05x |
| small/synthetic-small | clickhouse | indexed | olap_variable_length_reachability | 10.99±2.90 | 12.85±3.33 | 1.17x |
| small/synthetic-small | duckdb | indexed | olap_variable_length_grouped_max_rollup | 3.59±0.54 | 3.53±0.59 | 0.98x |
| small/synthetic-small | duckdb | indexed | olap_variable_length_grouped_rollup | 4.02±0.67 | 4.53±0.73 | 1.13x |
| small/synthetic-small | duckdb | indexed | olap_variable_length_reachability | 3.01±0.54 | 2.92±0.45 | 0.97x |
| small/synthetic-small | duckdb | unindexed | olap_variable_length_grouped_max_rollup | 3.13±0.48 | 3.18±0.80 | 1.02x |
| small/synthetic-small | duckdb | unindexed | olap_variable_length_grouped_rollup | 3.44±0.52 | 3.99±0.51 | 1.16x |
| small/synthetic-small | duckdb | unindexed | olap_variable_length_reachability | 2.73±0.52 | 3.06±0.83 | 1.12x |
| small/synthetic-small | postgresql | indexed | olap_variable_length_grouped_max_rollup | 1.45±0.16 | 0.84±0.14 | 0.58x |
| small/synthetic-small | postgresql | indexed | olap_variable_length_grouped_rollup | 4.90±0.74 | 6.00±1.24 | 1.22x |
| small/synthetic-small | postgresql | indexed | olap_variable_length_reachability | 1.41±0.16 | 0.81±0.12 | 0.58x |
| small/synthetic-small | postgresql | unindexed | olap_variable_length_grouped_max_rollup | 1.71±0.29 | 1.28±0.24 | 0.75x |
| small/synthetic-small | postgresql | unindexed | olap_variable_length_grouped_rollup | 4.45±0.46 | 5.48±0.76 | 1.23x |
| small/synthetic-small | postgresql | unindexed | olap_variable_length_reachability | 1.53±0.14 | 1.05±0.16 | 0.69x |
| small/synthetic-small | sqlite | indexed | olap_variable_length_grouped_max_rollup | 0.04±0.00 | 0.08±0.00 | 2.17x |
| small/synthetic-small | sqlite | indexed | olap_variable_length_grouped_rollup | 4.42±0.28 | 11.95±0.58 | 2.70x |
| small/synthetic-small | sqlite | indexed | olap_variable_length_reachability | 0.04±0.00 | 0.10±0.00 | 2.27x |
| small/synthetic-small | sqlite | unindexed | olap_variable_length_grouped_max_rollup | 0.83±0.05 | 0.63±0.04 | 0.76x |
| small/synthetic-small | sqlite | unindexed | olap_variable_length_grouped_rollup | 4.38±0.34 | 12.99±0.84 | 2.96x |
| small/synthetic-small | sqlite | unindexed | olap_variable_length_reachability | 0.80±0.06 | 0.63±0.04 | 0.78x |
| vendor-sizing/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_max_rollup | 40.97±1.77 | 0.93±0.17 | 0.02x |
| vendor-sizing/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_rollup | 9436.09±996.38 | 81477.09±6280.65 | 8.63x |
| vendor-sizing/synthetic-medium | postgresql | indexed | olap_variable_length_reachability | 40.42±2.07 | 0.77±0.16 | 0.02x |
| vendor-sizing/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_max_rollup | 439.94±41.02 | 241.36±37.69 | 0.55x |
| vendor-sizing/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_rollup | 9583.29±1068.59 | 78212.28±7010.68 | 8.16x |
| vendor-sizing/synthetic-medium | postgresql | unindexed | olap_variable_length_reachability | 442.15±45.29 | 243.22±38.16 | 0.55x |


## Outcome accounting (timeouts are data)

| dataset | engine | mode | workload | pass | timeout | fail |
|---|---|---|---|---|---|---|
| ladybug-0181/snb-sf1 | ladybug | unindexed | olap | 65 | 0 | 15 |
| ladybug-medium/ladybug-snb-sf1 | ladybug | unindexed | olap | 52 | 0 | 12 |
| large/snb-sf10 | clickhouse | indexed | olap | 36 | 9 | 9 |
| large/snb-sf10 | neo4j | unindexed | olap | 11 | 5 | 0 |
| large/synthetic-large | age | indexed | olap | 39 | 0 | 9 |
| large/synthetic-large | age | unindexed | olap | 39 | 0 | 9 |
| large/synthetic-large | neo4j | indexed | olap | 60 | 4 | 0 |
| large/synthetic-large | neo4j | unindexed | olap | 28 | 4 | 0 |
| large/synthetic-large | sqlite | unindexed | olap | 59 | 2 | 0 |
| medium/snb-sf1 | age | indexed | olap | 60 | 20 | 0 |
| medium/snb-sf1 | age | unindexed | olap | 60 | 20 | 0 |
| medium/snb-sf1 | arcadedb-embedded | indexed | olap | 75 | 5 | 0 |
| medium/snb-sf1 | arcadedb-embedded | unindexed | olap | 75 | 5 | 0 |
| medium/snb-sf1 | clickhouse | indexed | olap | 64 | 8 | 18 |
| medium/snb-sf1 | duckdb | indexed | olap | 68 | 9 | 0 |
| medium/snb-sf1 | duckdb | unindexed | olap | 69 | 8 | 0 |
| medium/snb-sf1 | neo4j | indexed | olap | 65 | 15 | 0 |
| medium/snb-sf1 | neo4j | unindexed | olap | 70 | 10 | 0 |
| medium/snb-sf1 | postgresql | indexed | olap | 70 | 25 | 0 |
| medium/snb-sf1 | postgresql | unindexed | olap | 70 | 25 | 0 |
| medium/snb-sf1 | sqlite | indexed | olap | 65 | 30 | 0 |
| medium/snb-sf1 | sqlite | unindexed | olap | 65 | 30 | 0 |
| medium/synthetic-medium | age | indexed | olap | 75 | 5 | 0 |
| medium/synthetic-medium | age | unindexed | olap | 75 | 5 | 0 |
| medium/synthetic-medium | arcadedb-embedded | indexed | olap | 70 | 10 | 0 |
| medium/synthetic-medium | arcadedb-embedded | unindexed | olap | 70 | 10 | 0 |
| medium/synthetic-medium | neo4j | unindexed | olap | 75 | 5 | 0 |
| medium/synthetic-medium | sqlite | indexed | olap | 94 | 1 | 0 |
| medium/synthetic-medium | sqlite | unindexed | olap | 93 | 2 | 0 |
| vendor-sizing/snb-sf1 | age | indexed | olap | 60 | 20 | 0 |
| vendor-sizing/snb-sf1 | age | unindexed | olap | 55 | 20 | 5 |
| vendor-sizing/snb-sf1 | neo4j | indexed | olap | 65 | 15 | 0 |
| vendor-sizing/snb-sf1 | neo4j | unindexed | olap | 70 | 10 | 0 |
| vendor-sizing/snb-sf1 | postgresql | indexed | olap | 70 | 25 | 0 |
| vendor-sizing/snb-sf1 | postgresql | unindexed | olap | 70 | 25 | 0 |
| vendor-sizing/synthetic-medium | age | indexed | olap | 75 | 5 | 0 |
| vendor-sizing/synthetic-medium | age | unindexed | olap | 75 | 5 | 0 |
| vendor-sizing/synthetic-medium | neo4j | unindexed | olap | 75 | 5 | 0 |


## Ingest time and on-disk size (mean±std over runs)

| dataset | engine | ingest s | db MiB |
|---|---|---|---|
| ladybug-0181/snb-sf1 | ladybug | 663.72±38.53 | 8834.87±62.25 |
| ladybug-0181/synthetic-medium | ladybug | 538.48±38.38 | 1746.45±2.31 |
| ladybug-0181/synthetic-small | ladybug | 0.59±0.09 | 10.45 |
| ladybug-medium/ladybug-snb-sf1 | ladybug | 4595.69±507.96 | 2458.79±6.82 |
| ladybug-medium/ladybug-synthetic-medium | ladybug | 4607.77±435.90 | 967.00±0.41 |
| large/snb-sf10 | clickhouse | 875.91±60.48 | 0.00 |
| large/snb-sf10 | neo4j | 8867.96 | - |
| large/synthetic-large | age | 2735.09±218.33 | - |
| large/synthetic-large | clickhouse | 746.14±48.85 | 0.00 |
| large/synthetic-large | duckdb | 254.25±7.27 | 13025.31±1209.98 |
| large/synthetic-large | neo4j | 9289.86±620.78 | - |
| large/synthetic-large | postgresql | 1593.56±113.24 | 0.00 |
| large/synthetic-large | sqlite | 1599.11±86.96 | 30901.56±1295.89 |
| medium/snb-sf1 | age | 447.96±22.22 | - |
| medium/snb-sf1 | arcadedb-embedded | 303.09±95.39 | 3049.92±151.08 |
| medium/snb-sf1 | clickhouse | 60.22±3.59 | 0.00 |
| medium/snb-sf1 | duckdb | 26.86±0.73 | 1263.62±163.42 |
| medium/snb-sf1 | neo4j | 693.42±55.61 | - |
| medium/snb-sf1 | postgresql | 148.23±9.33 | 0.00 |
| medium/snb-sf1 | sqlite | 110.58±5.95 | 2130.78±153.25 |
| medium/snb-sf1 | turso | 258.87±31.11 | 2164.42±175.83 |
| medium/synthetic-medium | age | 204.56±10.09 | - |
| medium/synthetic-medium | arcadedb-embedded | 214.21±22.25 | 2170.72±72.57 |
| medium/synthetic-medium | clickhouse | 39.27±2.20 | 0.00 |
| medium/synthetic-medium | duckdb | 17.67±0.40 | 610.64±353.25 |
| medium/synthetic-medium | neo4j | 513.36±39.25 | - |
| medium/synthetic-medium | postgresql | 107.14±5.66 | 0.00 |
| medium/synthetic-medium | sqlite | 53.72±2.81 | 1632.83±93.83 |
| medium/synthetic-medium | turso | 132.43±6.88 | 1684.35±114.80 |
| small/synthetic-small | age | 0.59±0.06 | - |
| small/synthetic-small | arcadedb-embedded | 0.74±0.23 | 8.02±3.88 |
| small/synthetic-small | clickhouse | 0.15±0.04 | 0.00 |
| small/synthetic-small | duckdb | 0.21±0.03 | 0.01 |
| small/synthetic-small | ladybug | 0.61±0.09 | 10.45 |
| small/synthetic-small | neo4j | 2.92±0.19 | - |
| small/synthetic-small | postgresql | 0.31±0.05 | 0.00 |
| small/synthetic-small | sqlite | 0.08±0.00 | 0.00 |
| small/synthetic-small | turso | 0.21±0.01 | 0.00 |
| vendor-sizing/snb-sf1 | age | 405.72±18.72 | - |
| vendor-sizing/snb-sf1 | neo4j | 676.46±53.35 | - |
| vendor-sizing/snb-sf1 | postgresql | 143.32±5.79 | 0.00 |
| vendor-sizing/synthetic-medium | age | 202.34±8.35 | - |
| vendor-sizing/synthetic-medium | neo4j | 510.05±34.38 | - |
| vendor-sizing/synthetic-medium | postgresql | 107.65±6.14 | 0.00 |

