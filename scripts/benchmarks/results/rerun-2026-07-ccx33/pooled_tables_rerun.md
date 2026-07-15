
## Engine x workload (pooled execute p50 ms, median [min-max] over repeats)

### ladybug-0181/snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 59.40 [54.65-81.48] | - | 342.18 [301.24-413.02] |

### ladybug-0181/synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 9.10 [4.58-10.86] | - | 2535.36 [2489.41-2758.91] |

### ladybug-0181/synthetic-small

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 0.89 [0.74-1.37] | - | 5.24 [4.19-5.88] |

### ladybug-medium/ladybug-snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 32.98 [32.24-39.96] (n=4) | - | 261.26 [247.17-328.99] (n=4) |

### ladybug-medium/ladybug-synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| ladybug | - | 9.05 [4.04-10.24] | - | 2461.47 [2449.36-2751.17] |

### large/snb-sf10

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| clickhouse | 820.53 [641.26-951.37] | - | 3343.61 [3014.27-3484.63] | - |
| neo4j | - | 74.44 [66.64-3190.98] (n=3) | - | 27471.50 (n=1) |

### large/synthetic-large

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| postgresql | 0.89 [0.78-1.68] | 546.76 [535.80-710.23] | 1507.36 [1292.07-16222.69] | 2898.66 [2750.32-13113.41] |
| duckdb | 3.92 [3.62-4.49] | 8.61 [7.78-9.45] | 5376.56 [5112.96-8332.68] | 5226.18 [5118.84-8189.84] |
| clickhouse | 85.30 [43.85-95.40] | - | 6248.17 [6045.59-7464.72] | - |
| sqlite | 0.13 [0.12-0.13] (n=6) | 819.50 [801.34-1104.49] (n=6) | 6309.96 [5990.06-7597.32] | 5112.77 [3996.42-12229.66] |
| neo4j | 1.20 [1.06-1.26] | 1591.53 [1347.85-1671.20] | 9378.49 [8714.44-9632.01] | 10386.10 [9558.13-10967.00] |
| age | 260.54 [227.46-387.28] | 818.35 [744.18-1054.57] | 13351.71 [12456.98-13840.29] | 13771.90 [12438.00-14118.84] |

### medium/snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| duckdb | 20.61 [19.36-23.63] | 6.06 [5.55-7.05] | 97.81 [89.85-14422.15] | 93.22 [90.67-16342.95] |
| arcadedb-embedded | 0.13 [0.13-0.16] | 247.15 [244.57-263.45] | 158.55 [156.62-179.43] | 159.79 [158.71-176.76] |
| neo4j | 3.16 [3.04-3.27] | 140.24 [136.34-224.55] | 3063.30 [2778.09-3440.63] | 2774.40 [2709.67-3132.69] |
| age | 4.94 [4.41-38.28] | 38.07 [7.91-60.99] | 3377.68 [3246.46-3832.87] | 3300.06 [3271.58-3796.41] |
| sqlite | 20.52 [19.53-23.19] | 126.34 [122.83-158.43] | 3905.70 [3825.56-4852.79] | 1395.94 [1379.99-1642.72] |
| clickhouse | 121.47 [66.39-129.33] | - | 4647.27 [376.48-4910.99] | - |
| turso | 31.52 [30.00-34.37] | 1749.14 [1535.83-1846.49] | 6227.08 [5738.40-7306.48] | 4724.84 [4427.25-5149.31] |
| postgresql | 9.29 [8.94-9.79] | 115.40 [111.24-143.85] | 9952.24 [9690.61-12851.32] | 12162.98 [10604.61-14432.52] |

### medium/synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| duckdb | 1.93 [1.72-2.39] | 3.55 [2.60-3.65] | 456.19 [453.07-559.98] | 464.81 [447.74-569.67] |
| clickhouse | 9.34 [7.26-10.95] | - | 514.93 [507.47-637.85] | - |
| age | 7.93 [7.57-9.02] | 30.81 [27.06-38.03] | 868.16 [859.60-1029.53] | 875.60 [872.13-1007.17] |
| arcadedb-embedded | 0.24 [0.21-0.28] | 59.59 [57.57-61.44] | 1643.72 [1507.12-2066.12] | 1569.87 [1459.20-2091.33] |
| turso | 0.27 [0.26-0.31] | 300.42 [294.10-342.51] | 2158.81 [2099.71-2426.49] | 3583.17 [3496.96-4044.48] |
| postgresql | 0.72 [0.51-0.99] | 55.06 [52.31-65.43] | 4734.96 [4645.70-5735.62] | 4761.11 [4715.46-6020.28] |
| neo4j | 0.89 [0.89-1.09] | 84.22 [77.87-91.76] | 13714.94 [13176.37-14690.73] | 14558.28 [13854.10-15688.67] |
| sqlite | 0.10 [0.10-0.11] | 66.57 [64.73-86.80] | 18331.11 [5810.53-18744.33] | 19216.05 [4754.86-19547.75] |

### small/synthetic-small

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| sqlite | 0.05 [0.05-0.05] | 0.13 [0.13-0.15] | 1.34 [1.32-1.49] | 1.48 [1.48-1.74] |
| postgresql | 0.67 [0.43-0.94] | 0.88 [0.60-1.14] | 1.86 [1.70-2.51] | 1.94 [1.75-2.43] |
| duckdb | 1.34 [1.14-1.46] | 1.18 [1.09-1.38] | 2.49 [2.03-2.80] | 2.03 [1.98-2.89] |
| turso | 0.34 [0.32-0.34] | 1.25 [1.23-1.37] | 3.29 [3.20-3.72] | 4.73 [4.59-5.17] |
| neo4j | 1.09 [1.01-1.31] | 1.26 [1.24-1.49] | 3.64 [3.38-4.47] | 3.62 [3.13-4.28] |
| arcadedb-embedded | 0.20 [0.18-0.21] | 1.30 [1.16-1.34] | 4.54 [3.99-4.65] | 4.04 [3.96-4.54] |
| ladybug | - | 1.44 [0.75-2.03] | - | 5.69 [4.05-6.97] |
| clickhouse | 3.14 [2.57-5.50] | - | 7.39 [4.19-8.45] | - |
| age | 0.56 [0.41-0.69] | 0.68 [0.53-1.07] | 70.81 [69.95-85.59] | 71.19 [69.82-85.36] |

### vendor-sizing/snb-sf1

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| neo4j | 3.18 [3.06-3.56] | 120.35 [113.24-122.45] | 1925.20 [1828.67-2060.77] | 1779.89 [1680.67-1894.30] |
| age | 11.72 [4.45-39.27] | 11.87 [7.77-38.42] | 3360.11 [3271.77-3747.86] | 2648.57 [2570.19-2980.91] |
| postgresql | 8.83 [8.42-9.74] | 99.75 [95.84-120.89] | 9558.03 [9445.74-12606.88] | 9936.46 [9799.12-12977.58] |

### vendor-sizing/synthetic-medium

| engine | OLTP idx | OLTP unidx | OLAP idx | OLAP unidx |
|---|---|---|---|---|
| age | 9.16 [8.34-9.42] | 32.54 [28.14-35.64] | 988.58 [947.99-1147.01] | 992.16 [965.07-1051.75] |
| postgresql | 0.72 [0.47-1.02] | 62.14 [60.67-66.15] | 4760.50 [4568.90-5635.17] | 4645.98 [4567.27-5727.64] |
| neo4j | 0.93 [0.86-1.35] | 85.38 [78.13-93.28] | 12788.60 [12465.78-14133.44] | 13335.39 [12943.43-14767.97] |


## Index-removal sensitivity (unindexed / indexed, execute p50)

| dataset | engine | OLTP ratio | OLAP ratio |
|---|---|---|---|
| large/synthetic-large | age | 3.1x | 1.0x |
| large/synthetic-large | duckdb | 2.2x | 1.0x |
| large/synthetic-large | neo4j | 1329.9x | 1.1x |
| large/synthetic-large | postgresql | 616.2x | 1.9x |
| large/synthetic-large | sqlite | 6491.5x | 0.8x |
| medium/snb-sf1 | age | 7.7x | 1.0x |
| medium/snb-sf1 | arcadedb-embedded | 1915.8x | 1.0x |
| medium/snb-sf1 | duckdb | 0.3x | 1.0x |
| medium/snb-sf1 | neo4j | 44.4x | 0.9x |
| medium/snb-sf1 | postgresql | 12.4x | 1.2x |
| medium/snb-sf1 | sqlite | 6.2x | 0.4x |
| medium/snb-sf1 | turso | 55.5x | 0.8x |
| medium/synthetic-medium | age | 3.9x | 1.0x |
| medium/synthetic-medium | arcadedb-embedded | 247.4x | 1.0x |
| medium/synthetic-medium | duckdb | 1.8x | 1.0x |
| medium/synthetic-medium | neo4j | 94.6x | 1.1x |
| medium/synthetic-medium | postgresql | 76.0x | 1.0x |
| medium/synthetic-medium | sqlite | 645.5x | 1.0x |
| medium/synthetic-medium | turso | 1100.1x | 1.7x |
| small/synthetic-small | age | 1.2x | 1.0x |
| small/synthetic-small | arcadedb-embedded | 6.4x | 0.9x |
| small/synthetic-small | duckdb | 0.9x | 0.8x |
| small/synthetic-small | neo4j | 1.2x | 1.0x |
| small/synthetic-small | postgresql | 1.3x | 1.0x |
| small/synthetic-small | sqlite | 2.7x | 1.1x |
| small/synthetic-small | turso | 3.7x | 1.4x |
| vendor-sizing/snb-sf1 | age | 1.0x | 0.8x |
| vendor-sizing/snb-sf1 | neo4j | 37.8x | 0.9x |
| vendor-sizing/snb-sf1 | postgresql | 11.3x | 1.0x |
| vendor-sizing/synthetic-medium | age | 3.6x | 1.0x |
| vendor-sizing/synthetic-medium | neo4j | 91.4x | 1.0x |
| vendor-sizing/synthetic-medium | postgresql | 86.7x | 1.0x |


## Variable-length lowering: unroll vs recursive CTE (execute p50 ms)

| dataset | engine | mode | query | unroll | rcte | rcte/unroll |
|---|---|---|---|---|---|---|
| large/synthetic-large | clickhouse | indexed | olap_variable_length_grouped_max_rollup | 6461.23 [6162.53-7256.04] | 3272.82 [2685.23-3832.72] | 0.51x |
| large/synthetic-large | clickhouse | indexed | olap_variable_length_grouped_rollup | 28637.98 [28262.77-34126.37] | 55746.97 [55041.46-69513.88] | 1.95x |
| large/synthetic-large | clickhouse | indexed | olap_variable_length_reachability | 6519.28 [5936.62-7213.63] | 3267.44 [2689.19-3751.13] | 0.50x |
| large/synthetic-large | duckdb | indexed | olap_variable_length_grouped_max_rollup | 664.16 [641.20-734.85] | 47.32 [37.36-50.86] | 0.07x |
| large/synthetic-large | duckdb | indexed | olap_variable_length_grouped_rollup | 11277.04 [11070.45-12998.73] | 84692.07 [79730.59-138517.42] | 7.51x |
| large/synthetic-large | duckdb | indexed | olap_variable_length_reachability | 814.48 [791.81-914.78] | 50.31 [47.51-53.16] | 0.06x |
| large/synthetic-large | duckdb | unindexed | olap_variable_length_grouped_max_rollup | 658.15 [633.99-727.88] | 53.41 [40.76-63.23] | 0.08x |
| large/synthetic-large | duckdb | unindexed | olap_variable_length_grouped_rollup | 11276.55 [11098.99-12944.93] | 82141.31 [79962.66-135824.63] | 7.28x |
| large/synthetic-large | duckdb | unindexed | olap_variable_length_reachability | 821.62 [775.00-913.55] | 53.36 [47.97-64.49] | 0.06x |
| large/synthetic-large | postgresql | indexed | olap_variable_length_grouped_max_rollup | 6959.33 (n=1) | 3.11 (n=1) | 0.00x |
| large/synthetic-large | postgresql | indexed | olap_variable_length_reachability | 7556.60 (n=1) | 1.90 [1.58-4.09] | 0.00x |
| large/synthetic-large | postgresql | unindexed | olap_variable_length_grouped_max_rollup | 24263.32 (n=1) | 8503.23 (n=1) | 0.35x |
| large/synthetic-large | postgresql | unindexed | olap_variable_length_reachability | 26721.42 (n=1) | 6525.09 [6310.75-8634.92] | 0.24x |
| large/synthetic-large | sqlite | indexed | olap_variable_length_grouped_max_rollup | 0.33 [0.33-0.34] (n=2) | 38.81 [38.61-39.01] (n=2) | 116.88x |
| large/synthetic-large | sqlite | indexed | olap_variable_length_reachability | 0.40 [0.40-0.40] (n=2) | 57.16 [44.06-58.13] | 143.21x |
| large/synthetic-large | sqlite | unindexed | olap_variable_length_grouped_max_rollup | 70245.13 (n=1) | 2781.58 [2523.09-3040.07] (n=2) | 0.04x |
| large/synthetic-large | sqlite | unindexed | olap_variable_length_reachability | 86397.49 [79145.04-93649.95] (n=2) | 2516.88 [2479.94-3022.05] | 0.03x |
| medium/snb-sf1 | duckdb | indexed | olap_variable_length_grouped_rollup | 29241.15 [26829.96-31652.35] (n=2) | 183455.93 (n=1) | 6.27x |
| medium/snb-sf1 | duckdb | unindexed | olap_variable_length_grouped_rollup | 28635.07 [25405.24-31864.90] (n=2) | 195363.30 [178693.55-212033.05] (n=2) | 6.82x |
| medium/synthetic-medium | clickhouse | indexed | olap_variable_length_grouped_max_rollup | 128.60 [124.20-154.19] | 98.58 [70.52-174.92] | 0.77x |
| medium/synthetic-medium | clickhouse | indexed | olap_variable_length_grouped_rollup | 2779.29 [2731.53-3336.91] | 5399.00 [5288.32-6794.17] | 1.94x |
| medium/synthetic-medium | clickhouse | indexed | olap_variable_length_reachability | 132.05 [130.17-150.97] | 100.45 [71.44-157.20] | 0.76x |
| medium/synthetic-medium | duckdb | indexed | olap_variable_length_grouped_max_rollup | 18.37 [17.80-23.82] | 8.32 [7.45-9.59] | 0.45x |
| medium/synthetic-medium | duckdb | indexed | olap_variable_length_grouped_rollup | 1289.44 [1237.76-1430.11] | 6756.11 [6728.87-8471.01] | 5.24x |
| medium/synthetic-medium | duckdb | indexed | olap_variable_length_reachability | 24.25 [18.81-31.09] | 10.42 [9.38-13.30] | 0.43x |
| medium/synthetic-medium | duckdb | unindexed | olap_variable_length_grouped_max_rollup | 20.66 [19.87-25.41] | 10.26 [9.36-12.04] | 0.50x |
| medium/synthetic-medium | duckdb | unindexed | olap_variable_length_grouped_rollup | 1278.91 [1241.80-1426.54] | 6899.34 [6610.41-8656.06] | 5.39x |
| medium/synthetic-medium | duckdb | unindexed | olap_variable_length_reachability | 23.84 [20.52-29.67] | 12.03 [10.09-14.13] | 0.50x |
| medium/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_max_rollup | 39.61 [39.15-42.76] | 1.00 [0.68-1.33] | 0.03x |
| medium/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_rollup | 9773.16 [9687.14-12617.58] | 78365.45 [76940.25-94250.60] | 8.02x |
| medium/synthetic-medium | postgresql | indexed | olap_variable_length_reachability | 39.36 [38.74-41.91] | 0.84 [0.71-1.40] | 0.02x |
| medium/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_max_rollup | 511.99 [477.37-668.35] | 344.51 [335.11-492.14] | 0.67x |
| medium/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_rollup | 9877.97 [9732.62-12755.13] | 75816.32 [75367.51-95648.71] | 7.68x |
| medium/synthetic-medium | postgresql | unindexed | olap_variable_length_reachability | 529.48 [491.51-712.84] | 354.97 [343.87-504.47] | 0.67x |
| medium/synthetic-medium | sqlite | indexed | olap_variable_length_grouped_max_rollup | 0.07 [0.07-0.10] | 2.62 [2.58-3.49] | 35.45x |
| medium/synthetic-medium | sqlite | indexed | olap_variable_length_grouped_rollup | 80143.11 [78604.01-95407.11] | 261571.62 [258886.27-268539.26] (n=4) | 3.26x |
| medium/synthetic-medium | sqlite | indexed | olap_variable_length_reachability | 0.11 [0.10-0.13] | 4.06 [3.92-5.50] | 37.24x |
| medium/synthetic-medium | sqlite | unindexed | olap_variable_length_grouped_max_rollup | 2311.19 [2211.23-2732.82] | 212.87 [207.82-252.14] | 0.09x |
| medium/synthetic-medium | sqlite | unindexed | olap_variable_length_grouped_rollup | 74908.85 [73596.23-89263.96] | 286478.73 [281802.21-287427.03] (n=3) | 3.82x |
| medium/synthetic-medium | sqlite | unindexed | olap_variable_length_reachability | 2736.80 [2650.09-3197.01] | 211.40 [206.73-250.05] | 0.08x |
| small/synthetic-small | clickhouse | indexed | olap_variable_length_grouped_max_rollup | 14.13 [6.16-16.39] | 12.25 [6.59-14.01] | 0.87x |
| small/synthetic-small | clickhouse | indexed | olap_variable_length_grouped_rollup | 12.27 [5.92-15.27] | 9.65 [7.21-15.85] | 0.79x |
| small/synthetic-small | clickhouse | indexed | olap_variable_length_reachability | 12.15 [5.44-13.86] | 13.90 [6.36-15.47] | 1.14x |
| small/synthetic-small | duckdb | indexed | olap_variable_length_grouped_max_rollup | 3.88 [2.72-4.11] | 3.32 [2.83-4.24] | 0.85x |
| small/synthetic-small | duckdb | indexed | olap_variable_length_grouped_rollup | 4.31 [3.21-4.79] | 4.71 [3.64-5.45] | 1.09x |
| small/synthetic-small | duckdb | indexed | olap_variable_length_reachability | 3.10 [2.34-3.86] | 2.83 [2.42-3.67] | 0.92x |
| small/synthetic-small | duckdb | unindexed | olap_variable_length_grouped_max_rollup | 3.05 [2.66-4.01] | 2.82 [2.67-4.78] | 0.93x |
| small/synthetic-small | duckdb | unindexed | olap_variable_length_grouped_rollup | 3.34 [2.95-4.39] | 3.81 [3.65-4.99] | 1.14x |
| small/synthetic-small | duckdb | unindexed | olap_variable_length_reachability | 2.48 [2.29-3.74] | 2.56 [2.48-4.64] | 1.03x |
| small/synthetic-small | postgresql | indexed | olap_variable_length_grouped_max_rollup | 1.40 [1.27-1.70] | 0.81 [0.65-1.06] | 0.58x |
| small/synthetic-small | postgresql | indexed | olap_variable_length_grouped_rollup | 4.59 [4.37-6.36] | 5.02 [4.97-7.67] | 1.09x |
| small/synthetic-small | postgresql | indexed | olap_variable_length_reachability | 1.35 [1.20-1.69] | 0.76 [0.65-0.99] | 0.56x |
| small/synthetic-small | postgresql | unindexed | olap_variable_length_grouped_max_rollup | 1.59 [1.41-2.07] | 1.24 [1.02-1.67] | 0.77x |
| small/synthetic-small | postgresql | unindexed | olap_variable_length_grouped_rollup | 4.27 [4.09-5.36] | 5.12 [4.95-6.97] | 1.20x |
| small/synthetic-small | postgresql | unindexed | olap_variable_length_reachability | 1.52 [1.37-1.78] | 1.03 [0.91-1.35] | 0.68x |
| small/synthetic-small | sqlite | indexed | olap_variable_length_grouped_max_rollup | 0.04 [0.04-0.04] | 0.08 [0.08-0.08] | 2.17x |
| small/synthetic-small | sqlite | indexed | olap_variable_length_grouped_rollup | 4.28 [4.26-4.99] | 11.71 [11.57-13.10] | 2.74x |
| small/synthetic-small | sqlite | indexed | olap_variable_length_reachability | 0.04 [0.04-0.04] | 0.10 [0.10-0.11] | 2.21x |
| small/synthetic-small | sqlite | unindexed | olap_variable_length_grouped_max_rollup | 0.80 [0.80-0.94] | 0.61 [0.61-0.72] | 0.77x |
| small/synthetic-small | sqlite | unindexed | olap_variable_length_grouped_rollup | 4.20 [4.18-5.07] | 12.59 [12.55-14.67] | 3.00x |
| small/synthetic-small | sqlite | unindexed | olap_variable_length_reachability | 0.77 [0.77-0.92] | 0.61 [0.61-0.72] | 0.79x |
| vendor-sizing/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_max_rollup | 41.99 [38.18-42.63] | 0.91 [0.69-1.20] | 0.02x |
| vendor-sizing/synthetic-medium | postgresql | indexed | olap_variable_length_grouped_rollup | 8992.90 [8661.06-11396.34] | 79933.18 [76559.03-93670.58] | 8.89x |
| vendor-sizing/synthetic-medium | postgresql | indexed | olap_variable_length_reachability | 40.53 [37.22-43.63] | 0.67 [0.61-1.05] | 0.02x |
| vendor-sizing/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_max_rollup | 419.22 [410.64-520.93] | 223.33 [220.54-316.71] | 0.53x |
| vendor-sizing/synthetic-medium | postgresql | unindexed | olap_variable_length_grouped_rollup | 9111.27 [8914.79-11714.13] | 75194.06 [73981.77-92201.63] | 8.25x |
| vendor-sizing/synthetic-medium | postgresql | unindexed | olap_variable_length_reachability | 421.21 [410.23-531.98] | 227.02 [216.59-318.52] | 0.54x |


## Outcome accounting (timeouts are data)

| dataset | engine | mode | workload | pass | timeout | fail |
|---|---|---|---|---|---|---|
| ladybug-0181/snb-sf1 | ladybug | unindexed | olap | 65 | 0 | 15 |
| ladybug-medium/ladybug-snb-sf1 | ladybug | unindexed | olap | 52 | 0 | 12 |
| large/snb-sf10 | clickhouse | indexed | olap | 60 | 15 | 15 |
| large/snb-sf10 | neo4j | unindexed | olap | 11 | 5 | 0 |
| large/snb-sf10 | neo4j | unindexed | oltp | 38 | 4 | 0 |
| large/synthetic-large | age | indexed | olap | 65 | 0 | 15 |
| large/synthetic-large | age | unindexed | olap | 65 | 0 | 15 |
| large/synthetic-large | neo4j | indexed | olap | 75 | 5 | 0 |
| large/synthetic-large | neo4j | unindexed | olap | 70 | 10 | 0 |
| large/synthetic-large | postgresql | indexed | olap | 74 | 1 | 0 |
| large/synthetic-large | postgresql | unindexed | olap | 74 | 1 | 0 |
| large/synthetic-large | sqlite | indexed | olap | 76 | 4 | 0 |
| large/synthetic-large | sqlite | unindexed | olap | 75 | 3 | 0 |
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


## Ingest time and on-disk size (median [min-max] over runs)

| dataset | engine | ingest s | db MiB |
|---|---|---|---|
| ladybug-0181/snb-sf1 | ladybug | 644.06 [632.56-742.03] (n=10) | 8809.84 [8749.32-8953.98] (n=10) |
| ladybug-0181/synthetic-medium | ladybug | 522.35 [507.94-615.22] (n=10) | 1746.62 [1741.28-1749.50] (n=10) |
| ladybug-0181/synthetic-small | ladybug | 0.57 [0.46-0.72] (n=10) | 10.45 [10.45-10.45] (n=10) |
| ladybug-medium/ladybug-snb-sf1 | ladybug | 4311.65 [4279.05-5541.33] (n=8) | 2460.37 [2443.82-2468.16] (n=8) |
| ladybug-medium/ladybug-synthetic-medium | ladybug | 4415.34 [4331.60-5483.74] (n=10) | 966.83 [966.68-968.14] (n=10) |
| large/snb-sf10 | clickhouse | 956.82 [815.25-995.70] (n=10) | 0.00 [0.00-0.00] (n=10) |
| large/snb-sf10 | neo4j | 10026.88 [8867.96-11264.41] (n=4) | - |
| large/synthetic-large | age | 3042.85 [2556.36-3103.86] (n=20) | - |
| large/synthetic-large | clickhouse | 746.06 [697.14-865.10] (n=10) | 0.00 [0.00-0.00] (n=10) |
| large/synthetic-large | duckdb | 255.05 [240.15-266.40] (n=20) | 13026.14 [11806.26-14239.01] (n=20) |
| large/synthetic-large | neo4j | 10144.03 [8585.74-10358.80] (n=20) | - |
| large/synthetic-large | postgresql | 1672.26 [1502.55-3317.25] (n=20) | 0.00 [0.00-0.00] (n=20) |
| large/synthetic-large | sqlite | 1609.85 [1455.92-1821.58] (n=22) | 31064.82 [29758.69-32370.95] (n=22) |
| medium/snb-sf1 | age | 435.64 [428.15-488.75] (n=20) | - |
| medium/snb-sf1 | arcadedb-embedded | 287.69 [246.54-702.46] (n=20) | 3041.36 [2899.32-3217.64] (n=20) |
| medium/snb-sf1 | clickhouse | 58.98 [55.33-66.90] (n=10) | 0.00 [0.00-0.00] (n=10) |
| medium/snb-sf1 | duckdb | 26.92 [25.67-28.41] (n=20) | 1263.64 [1098.76-1429.01] (n=20) |
| medium/snb-sf1 | neo4j | 687.38 [608.98-786.71] (n=20) | - |
| medium/snb-sf1 | postgresql | 145.59 [137.98-173.20] (n=20) | 0.00 [0.00-0.00] (n=20) |
| medium/snb-sf1 | sqlite | 109.51 [102.13-120.46] (n=20) | 2130.78 [1977.52-2284.03] (n=20) |
| medium/snb-sf1 | turso | 255.11 [225.48-368.33] (n=20) | 2164.42 [1988.59-2340.25] (n=20) |
| medium/synthetic-medium | age | 199.37 [195.90-224.64] (n=20) | - |
| medium/synthetic-medium | arcadedb-embedded | 219.41 [189.07-268.67] (n=20) | 2170.41 [2098.15-2243.91] (n=20) |
| medium/synthetic-medium | clickhouse | 38.72 [37.40-44.02] (n=10) | 0.00 [0.00-0.00] (n=10) |
| medium/synthetic-medium | duckdb | 17.75 [16.70-18.20] (n=20) | 610.76 [256.26-965.26] (n=20) |
| medium/synthetic-medium | neo4j | 498.18 [465.65-585.58] (n=20) | - |
| medium/synthetic-medium | postgresql | 104.45 [102.64-118.55] (n=20) | 0.00 [0.00-0.00] (n=20) |
| medium/synthetic-medium | sqlite | 53.08 [50.76-59.09] (n=20) | 1632.83 [1539.00-1726.66] (n=20) |
| medium/synthetic-medium | turso | 129.93 [126.63-146.56] (n=20) | 1684.35 [1569.55-1799.15] (n=20) |
| small/synthetic-small | age | 0.60 [0.48-0.69] (n=20) | - |
| small/synthetic-small | arcadedb-embedded | 0.76 [0.39-1.07] (n=20) | 8.02 [4.14-11.89] (n=20) |
| small/synthetic-small | clickhouse | 0.16 [0.09-0.22] (n=10) | 0.00 [0.00-0.00] (n=10) |
| small/synthetic-small | duckdb | 0.20 [0.17-0.26] (n=20) | 0.01 [0.01-0.01] (n=20) |
| small/synthetic-small | ladybug | 0.61 [0.46-0.77] (n=10) | 10.45 [10.45-10.45] (n=10) |
| small/synthetic-small | neo4j | 2.84 [2.76-3.39] (n=20) | - |
| small/synthetic-small | postgresql | 0.33 [0.21-0.40] (n=20) | 0.00 [0.00-0.00] (n=20) |
| small/synthetic-small | sqlite | 0.08 [0.08-0.09] (n=20) | 0.00 [0.00-0.00] (n=20) |
| small/synthetic-small | turso | 0.21 [0.20-0.24] (n=20) | 0.00 [0.00-0.00] (n=20) |
| vendor-sizing/snb-sf1 | age | 396.02 [388.08-440.63] (n=20) | - |
| vendor-sizing/snb-sf1 | neo4j | 671.56 [603.51-773.41] (n=20) | - |
| vendor-sizing/snb-sf1 | postgresql | 141.72 [135.39-155.83] (n=20) | 0.00 [0.00-0.00] (n=20) |
| vendor-sizing/synthetic-medium | age | 201.37 [191.08-217.63] (n=20) | - |
| vendor-sizing/synthetic-medium | neo4j | 501.83 [458.15-569.48] (n=20) | - |
| vendor-sizing/synthetic-medium | postgresql | 105.91 [101.07-120.71] (n=20) | 0.00 [0.00-0.00] (n=20) |

