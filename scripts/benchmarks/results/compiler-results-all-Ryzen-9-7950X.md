# Compiler Benchmark Summary

Source: `scripts/benchmarks/results/compiler_benchmark.json`  
Generated: `2026-04-24T10:37:15.737312+00:00`  
CypherGlot: `0.0.1.dev14`  
Python: `3.12.11`  
Platform: `Linux-6.17.9-76061709-generic-x86_64-with-glibc2.35`

## Overview

- Corpus: `/mnt/ssd2/repos/cypherglot/scripts/benchmarks/corpora/compiler_benchmark_corpus.json`
- Query count: `22`
- Schema layout: `type-aware`
- SQL emission: `strict-relational`
- Release subset: `admitted-v0.1.0`
- Node types: `User, Company, Person`
- Edge types: `KNOWS, WORKS_AT, INTRODUCED`
- SQL corpus: `/mnt/ssd2/repos/cypherglot/scripts/benchmarks/corpora/compiler_sqlglot_benchmark_corpus.json`
- SQL query count: `22`
- SQLGlot mode: `both`

## Benchmark Sections

### `shared_entrypoint_results`

- Purpose: `Measures backend-neutral public compiler entrypoints once per query.`
- Comparison axis: `shared compiler frontend behavior`
- Entrypoints: `parse_cypher_text, validate_cypher_text, normalize_cypher_text`

### `backend_entrypoint_results`

- Purpose: `Measures backend-dependent public compiler entrypoints once per SQL backend.`
- Comparison axis: `end-to-end public API SQL target behavior`
- Entrypoints: `render_cypher_program_text, to_sql, to_sqlglot_ast, to_sqlglot_program`
- Backends: `sqlite, duckdb, postgresql`

### `backend_lowering_results`

- Purpose: `Measures shared IR binding, lowering, and rendering cost per SQL backend below the public API layer.`
- Comparison axis: `backend lowerer and renderer behavior`
- Backends: `sqlite, duckdb, postgresql`

## Shared Entrypoints

| Entrypoint | Queries | Iterations | Warmup | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| parse_cypher_text | 22 | 10000 | 200 | `540.10 us` | `539.68 us` | `897.87 us` | `933.24 us` |
| validate_cypher_text | 22 | 10000 | 200 | `605.09 us` | `639.12 us` | `1006.27 us` | `1044.15 us` |
| normalize_cypher_text | 22 | 10000 | 200 | `690.60 us` | `701.14 us` | `1136.08 us` | `1199.41 us` |

## Shared Entrypoints Per Query

### `parse_cypher_text` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `463.09 us` | `456.18 us` | `501.81 us` | `612.75 us` |
| relationship_projection | one-hop-read | `638.93 us` | `634.87 us` | `658.41 us` | `742.79 us` |
| optional_match_string_filter | optional-match | `429.51 us` | `426.36 us` | `442.71 us` | `507.43 us` |
| optional_match_grouped_count | optional-match | `443.60 us` | `441.20 us` | `456.48 us` | `510.15 us` |
| with_scalar_rebinding | with-return | `361.21 us` | `357.85 us` | `379.71 us` | `450.10 us` |
| with_grouped_aggregate | aggregation | `587.72 us` | `584.23 us` | `608.57 us` | `662.13 us` |
| variable_length_projection | variable-length | `506.65 us` | `504.04 us` | `521.53 us` | `571.96 us` |
| variable_length_grouped_count | variable-length | `553.88 us` | `549.91 us` | `575.09 us` | `646.91 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `591.94 us` | `588.19 us` | `609.62 us` | `683.08 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `554.07 us` | `552.46 us` | `568.60 us` | `591.60 us` |
| searched_case_projection | computed-projection | `700.64 us` | `694.73 us` | `734.07 us` | `823.78 us` |
| graph_introspection | introspection | `816.09 us` | `809.20 us` | `855.56 us` | `989.37 us` |
| metadata_projection | metadata-projection | `923.83 us` | `917.78 us` | `959.89 us` | `1081.41 us` |
| relationship_property_functions | computed-projection | `863.10 us` | `854.20 us` | `916.81 us` | `1006.32 us` |
| unwind_literal_list | unwind | `471.16 us` | `466.28 us` | `492.76 us` | `573.77 us` |
| create_node_program | write-program | `225.50 us` | `224.08 us` | `236.02 us` | `257.36 us` |
| traversal_create_program | write-program | `408.52 us` | `406.94 us` | `420.79 us` | `444.39 us` |
| traversal_merge_program | write-program | `407.16 us` | `404.80 us` | `419.85 us` | `480.14 us` |
| match_set_node | write-single-statement | `326.47 us` | `325.20 us` | `336.55 us` | `351.43 us` |
| match_delete_relationship | write-single-statement | `280.96 us` | `280.19 us` | `289.36 us` | `297.53 us` |
| vector_query_nodes_match | vector-aware | `635.10 us` | `633.99 us` | `649.07 us` | `665.93 us` |
| vector_query_nodes_yield_where | vector-aware | `693.13 us` | `688.62 us` | `714.82 us` | `800.36 us` |

### `validate_cypher_text` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `501.35 us` | `499.23 us` | `514.35 us` | `545.51 us` |
| relationship_projection | one-hop-read | `739.94 us` | `734.33 us` | `765.97 us` | `873.53 us` |
| optional_match_string_filter | optional-match | `478.53 us` | `476.88 us` | `491.98 us` | `513.05 us` |
| optional_match_grouped_count | optional-match | `502.52 us` | `501.42 us` | `514.20 us` | `527.14 us` |
| with_scalar_rebinding | with-return | `399.18 us` | `397.22 us` | `410.29 us` | `445.58 us` |
| with_grouped_aggregate | aggregation | `644.20 us` | `639.87 us` | `668.66 us` | `746.71 us` |
| variable_length_projection | variable-length | `643.67 us` | `637.92 us` | `677.31 us` | `767.74 us` |
| variable_length_grouped_count | variable-length | `675.28 us` | `664.11 us` | `745.14 us` | `859.11 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `681.05 us` | `674.52 us` | `711.73 us` | `825.50 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `699.49 us` | `692.00 us` | `744.37 us` | `860.34 us` |
| searched_case_projection | computed-projection | `762.36 us` | `755.20 us` | `804.86 us` | `896.40 us` |
| graph_introspection | introspection | `920.43 us` | `911.11 us` | `975.67 us` | `1094.27 us` |
| metadata_projection | metadata-projection | `1033.26 us` | `1024.93 us` | `1087.80 us` | `1204.41 us` |
| relationship_property_functions | computed-projection | `973.02 us` | `962.03 us` | `1045.44 us` | `1166.22 us` |
| unwind_literal_list | unwind | `503.11 us` | `496.53 us` | `537.10 us` | `673.78 us` |
| create_node_program | write-program | `233.05 us` | `229.32 us` | `249.82 us` | `329.44 us` |
| traversal_create_program | write-program | `447.94 us` | `446.30 us` | `459.97 us` | `488.87 us` |
| traversal_merge_program | write-program | `453.19 us` | `449.49 us` | `469.73 us` | `551.07 us` |
| match_set_node | write-single-statement | `339.55 us` | `335.15 us` | `365.37 us` | `433.50 us` |
| match_delete_relationship | write-single-statement | `292.21 us` | `290.66 us` | `301.70 us` | `333.22 us` |
| vector_query_nodes_match | vector-aware | `668.37 us` | `666.09 us` | `685.09 us` | `722.65 us` |
| vector_query_nodes_yield_where | vector-aware | `720.39 us` | `714.58 us` | `753.10 us` | `836.18 us` |

### `normalize_cypher_text` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `556.08 us` | `550.54 us` | `581.98 us` | `684.66 us` |
| relationship_projection | one-hop-read | `826.85 us` | `816.14 us` | `892.36 us` | `1014.94 us` |
| optional_match_string_filter | optional-match | `527.30 us` | `521.95 us` | `553.80 us` | `633.62 us` |
| optional_match_grouped_count | optional-match | `543.55 us` | `540.50 us` | `561.49 us` | `607.65 us` |
| with_scalar_rebinding | with-return | `443.46 us` | `438.56 us` | `464.00 us` | `578.58 us` |
| with_grouped_aggregate | aggregation | `709.72 us` | `699.53 us` | `768.11 us` | `912.52 us` |
| variable_length_projection | variable-length | `708.54 us` | `701.98 us` | `749.93 us` | `837.32 us` |
| variable_length_grouped_count | variable-length | `723.22 us` | `714.65 us` | `777.36 us` | `848.71 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `761.34 us` | `754.31 us` | `794.79 us` | `914.47 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `765.62 us` | `757.14 us` | `809.12 us` | `944.16 us` |
| searched_case_projection | computed-projection | `826.73 us` | `818.87 us` | `869.97 us` | `974.89 us` |
| graph_introspection | introspection | `987.09 us` | `981.72 us` | `1017.86 us` | `1123.86 us` |
| metadata_projection | metadata-projection | `1104.87 us` | `1099.90 us` | `1130.48 us` | `1228.92 us` |
| relationship_property_functions | computed-projection | `1043.51 us` | `1038.38 us` | `1071.55 us` | `1170.75 us` |
| unwind_literal_list | unwind | `539.53 us` | `528.53 us` | `614.74 us` | `742.11 us` |
| create_node_program | write-program | `252.95 us` | `248.65 us` | `278.84 us` | `336.69 us` |
| traversal_create_program | write-program | `501.22 us` | `490.81 us` | `562.69 us` | `684.69 us` |
| traversal_merge_program | write-program | `495.04 us` | `486.25 us` | `549.21 us` | `650.40 us` |
| match_set_node | write-single-statement | `374.26 us` | `369.03 us` | `403.70 us` | `477.81 us` |
| match_delete_relationship | write-single-statement | `317.44 us` | `312.97 us` | `343.65 us` | `408.85 us` |
| vector_query_nodes_match | vector-aware | `1008.50 us` | `986.61 us` | `1134.40 us` | `1290.17 us` |
| vector_query_nodes_yield_where | vector-aware | `1176.29 us` | `1157.13 us` | `1290.02 us` | `1410.49 us` |

## Backend Entrypoints

| Backend | Entrypoint | Queries | Iterations | Warmup | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sqlite | to_sqlglot_ast | 15 | 10000 | 200 | `924.16 us` | `942.27 us` | `1276.29 us` | `1366.40 us` |
| duckdb | to_sqlglot_ast | 15 | 10000 | 200 | `932.11 us` | `955.34 us` | `1286.10 us` | `1409.37 us` |
| postgresql | to_sqlglot_ast | 15 | 10000 | 200 | `917.77 us` | `945.26 us` | `1266.37 us` | `1298.53 us` |
| sqlite | to_sql | 15 | 10000 | 200 | `1038.39 us` | `1081.95 us` | `1388.99 us` | `1426.78 us` |
| duckdb | to_sql | 15 | 10000 | 200 | `1045.16 us` | `1072.89 us` | `1428.36 us` | `1515.06 us` |
| postgresql | to_sql | 15 | 10000 | 200 | `1044.30 us` | `1075.33 us` | `1405.18 us` | `1517.93 us` |
| sqlite | to_sqlglot_program | 20 | 10000 | 200 | `822.78 us` | `846.27 us` | `1269.44 us` | `1333.98 us` |
| duckdb | to_sqlglot_program | 20 | 10000 | 200 | `828.47 us` | `847.98 us` | `1268.26 us` | `1347.56 us` |
| postgresql | to_sqlglot_program | 20 | 10000 | 200 | `829.97 us` | `849.90 us` | `1272.52 us` | `1379.57 us` |
| sqlite | render_cypher_program_text | 20 | 10000 | 200 | `958.32 us` | `967.15 us` | `1415.39 us` | `1532.10 us` |
| duckdb | render_cypher_program_text | 20 | 10000 | 200 | `946.98 us` | `955.06 us` | `1417.97 us` | `1496.88 us` |
| postgresql | render_cypher_program_text | 20 | 10000 | 200 | `951.80 us` | `961.79 us` | `1404.22 us` | `1511.92 us` |

## Backend Entrypoints Per Query

### `sqlite` / `to_sqlglot_ast` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `685.11 us` | `669.26 us` | `784.43 us` | `905.10 us` |
| relationship_projection | one-hop-read | `977.54 us` | `962.13 us` | `1085.55 us` | `1231.60 us` |
| optional_match_string_filter | optional-match | `692.46 us` | `680.61 us` | `773.78 us` | `864.77 us` |
| optional_match_grouped_count | optional-match | `707.17 us` | `697.73 us` | `771.67 us` | `855.86 us` |
| with_scalar_rebinding | with-return | `559.82 us` | `547.76 us` | `641.17 us` | `730.08 us` |
| with_grouped_aggregate | aggregation | `870.93 us` | `861.03 us` | `933.16 us` | `1034.35 us` |
| variable_length_projection | variable-length | `954.57 us` | `938.15 us` | `1054.83 us` | `1200.99 us` |
| variable_length_grouped_count | variable-length | `1003.71 us` | `986.64 us` | `1100.98 us` | `1241.87 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `922.38 us` | `909.89 us` | `999.69 us` | `1173.12 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1037.36 us` | `1019.57 us` | `1143.73 us` | `1295.79 us` |
| searched_case_projection | computed-projection | `1039.10 us` | `1025.26 us` | `1126.53 us` | `1280.82 us` |
| graph_introspection | introspection | `1166.90 us` | `1145.45 us` | `1286.21 us` | `1508.03 us` |
| metadata_projection | metadata-projection | `1289.39 us` | `1276.22 us` | `1379.12 us` | `1500.05 us` |
| relationship_property_functions | computed-projection | `1255.36 us` | `1238.24 us` | `1358.76 us` | `1508.78 us` |
| unwind_literal_list | unwind | `700.58 us` | `677.89 us` | `829.20 us` | `965.71 us` |

### `duckdb` / `to_sqlglot_ast` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `671.16 us` | `664.72 us` | `718.81 us` | `803.78 us` |
| relationship_projection | one-hop-read | `983.02 us` | `968.06 us` | `1084.22 us` | `1217.29 us` |
| optional_match_string_filter | optional-match | `702.15 us` | `687.75 us` | `788.08 us` | `913.31 us` |
| optional_match_grouped_count | optional-match | `714.88 us` | `700.29 us` | `805.03 us` | `946.65 us` |
| with_scalar_rebinding | with-return | `573.82 us` | `559.61 us` | `667.23 us` | `774.18 us` |
| with_grouped_aggregate | aggregation | `903.57 us` | `875.57 us` | `1041.65 us` | `1270.30 us` |
| variable_length_projection | variable-length | `967.29 us` | `949.03 us` | `1096.81 us` | `1236.79 us` |
| variable_length_grouped_count | variable-length | `1010.19 us` | `993.37 us` | `1110.54 us` | `1235.00 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `938.93 us` | `918.76 us` | `1052.09 us` | `1235.15 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1040.74 us` | `1020.14 us` | `1154.42 us` | `1315.25 us` |
| searched_case_projection | computed-projection | `1051.25 us` | `1032.74 us` | `1161.08 us` | `1289.64 us` |
| graph_introspection | introspection | `1173.38 us` | `1149.74 us` | `1296.60 us` | `1517.00 us` |
| metadata_projection | metadata-projection | `1302.71 us` | `1280.26 us` | `1427.69 us` | `1572.66 us` |
| relationship_property_functions | computed-projection | `1269.63 us` | `1245.74 us` | `1397.73 us` | `1541.00 us` |
| unwind_literal_list | unwind | `678.88 us` | `668.81 us` | `757.06 us` | `824.32 us` |

### `postgresql` / `to_sqlglot_ast` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `680.09 us` | `669.03 us` | `757.37 us` | `892.44 us` |
| relationship_projection | one-hop-read | `977.11 us` | `963.33 us` | `1064.86 us` | `1237.18 us` |
| optional_match_string_filter | optional-match | `689.39 us` | `681.71 us` | `739.80 us` | `844.14 us` |
| optional_match_grouped_count | optional-match | `713.14 us` | `699.55 us` | `798.90 us` | `957.84 us` |
| with_scalar_rebinding | with-return | `562.33 us` | `550.51 us` | `643.94 us` | `738.62 us` |
| with_grouped_aggregate | aggregation | `872.39 us` | `862.93 us` | `928.55 us` | `1043.09 us` |
| variable_length_projection | variable-length | `954.51 us` | `942.50 us` | `1048.03 us` | `1171.24 us` |
| variable_length_grouped_count | variable-length | `1003.30 us` | `988.62 us` | `1086.00 us` | `1239.49 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `924.41 us` | `912.17 us` | `993.14 us` | `1157.76 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1021.39 us` | `1011.50 us` | `1084.80 us` | `1153.59 us` |
| searched_case_projection | computed-projection | `1038.76 us` | `1026.74 us` | `1119.20 us` | `1198.89 us` |
| graph_introspection | introspection | `1152.36 us` | `1143.43 us` | `1214.42 us` | `1316.64 us` |
| metadata_projection | metadata-projection | `1277.17 us` | `1271.36 us` | `1314.33 us` | `1390.76 us` |
| relationship_property_functions | computed-projection | `1234.29 us` | `1229.78 us` | `1263.29 us` | `1344.15 us` |
| unwind_literal_list | unwind | `665.85 us` | `662.79 us` | `688.04 us` | `724.32 us` |

### `sqlite` / `to_sql` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `740.99 us` | `739.91 us` | `765.20 us` | `788.80 us` |
| relationship_projection | one-hop-read | `1084.42 us` | `1079.23 us` | `1130.59 us` | `1284.08 us` |
| optional_match_string_filter | optional-match | `786.03 us` | `780.97 us` | `811.64 us` | `885.56 us` |
| optional_match_grouped_count | optional-match | `800.01 us` | `793.56 us` | `844.82 us` | `915.67 us` |
| with_scalar_rebinding | with-return | `629.56 us` | `625.52 us` | `654.42 us` | `725.25 us` |
| with_grouped_aggregate | aggregation | `962.17 us` | `958.56 us` | `990.18 us` | `1038.92 us` |
| variable_length_projection | variable-length | `1150.45 us` | `1147.92 us` | `1189.06 us` | `1334.44 us` |
| variable_length_grouped_count | variable-length | `1219.75 us` | `1212.27 us` | `1256.81 us` | `1343.00 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `1041.43 us` | `1034.98 us` | `1077.49 us` | `1174.78 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1255.01 us` | `1244.32 us` | `1305.92 us` | `1443.85 us` |
| searched_case_projection | computed-projection | `1126.58 us` | `1120.17 us` | `1157.57 us` | `1261.49 us` |
| graph_introspection | introspection | `1247.31 us` | `1244.06 us` | `1272.88 us` | `1319.97 us` |
| metadata_projection | metadata-projection | `1394.50 us` | `1384.96 us` | `1460.03 us` | `1568.47 us` |
| relationship_property_functions | computed-projection | `1386.30 us` | `1380.78 us` | `1418.75 us` | `1508.42 us` |
| unwind_literal_list | unwind | `751.30 us` | `745.45 us` | `779.30 us` | `902.78 us` |

### `duckdb` / `to_sql` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `730.15 us` | `727.89 us` | `753.83 us` | `810.59 us` |
| relationship_projection | one-hop-read | `1078.06 us` | `1069.32 us` | `1150.87 us` | `1291.53 us` |
| optional_match_string_filter | optional-match | `779.56 us` | `773.09 us` | `810.97 us` | `913.17 us` |
| optional_match_grouped_count | optional-match | `788.84 us` | `785.21 us` | `813.56 us` | `877.97 us` |
| with_scalar_rebinding | with-return | `626.29 us` | `618.24 us` | `672.91 us` | `797.82 us` |
| with_grouped_aggregate | aggregation | `964.11 us` | `956.98 us` | `998.00 us` | `1175.35 us` |
| variable_length_projection | variable-length | `1150.91 us` | `1145.37 us` | `1217.37 us` | `1373.23 us` |
| variable_length_grouped_count | variable-length | `1215.62 us` | `1203.49 us` | `1281.69 us` | `1407.31 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `1035.60 us` | `1027.47 us` | `1081.37 us` | `1211.35 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1274.63 us` | `1255.98 us` | `1380.47 us` | `1542.95 us` |
| searched_case_projection | computed-projection | `1137.40 us` | `1122.97 us` | `1226.19 us` | `1357.53 us` |
| graph_introspection | introspection | `1304.93 us` | `1269.92 us` | `1485.00 us` | `1836.35 us` |
| metadata_projection | metadata-projection | `1399.74 us` | `1387.32 us` | `1473.23 us` | `1640.82 us` |
| relationship_property_functions | computed-projection | `1441.44 us` | `1429.25 us` | `1518.85 us` | `1665.59 us` |
| unwind_literal_list | unwind | `750.18 us` | `745.42 us` | `779.24 us` | `855.48 us` |

### `postgresql` / `to_sql` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `731.19 us` | `723.79 us` | `781.73 us` | `872.36 us` |
| relationship_projection | one-hop-read | `1080.98 us` | `1072.08 us` | `1148.88 us` | `1296.13 us` |
| optional_match_string_filter | optional-match | `789.15 us` | `780.41 us` | `837.37 us` | `959.48 us` |
| optional_match_grouped_count | optional-match | `810.05 us` | `794.69 us` | `900.05 us` | `1051.15 us` |
| with_scalar_rebinding | with-return | `633.48 us` | `626.51 us` | `679.05 us` | `778.92 us` |
| with_grouped_aggregate | aggregation | `968.74 us` | `960.75 us` | `1022.31 us` | `1128.92 us` |
| variable_length_projection | variable-length | `1153.85 us` | `1147.72 us` | `1224.10 us` | `1383.85 us` |
| variable_length_grouped_count | variable-length | `1222.92 us` | `1211.37 us` | `1289.31 us` | `1417.64 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `1034.13 us` | `1030.12 us` | `1061.13 us` | `1131.13 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1260.72 us` | `1247.56 us` | `1341.65 us` | `1486.25 us` |
| searched_case_projection | computed-projection | `1131.56 us` | `1121.69 us` | `1198.13 us` | `1299.70 us` |
| graph_introspection | introspection | `1255.15 us` | `1242.60 us` | `1334.43 us` | `1498.18 us` |
| metadata_projection | metadata-projection | `1412.89 us` | `1394.58 us` | `1521.97 us` | `1671.35 us` |
| relationship_property_functions | computed-projection | `1415.33 us` | `1391.30 us` | `1547.20 us` | `1716.90 us` |
| unwind_literal_list | unwind | `764.33 us` | `752.04 us` | `844.36 us` | `966.06 us` |

### `sqlite` / `to_sqlglot_program` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `668.47 us` | `656.73 us` | `744.54 us` | `869.85 us` |
| relationship_projection | one-hop-read | `988.05 us` | `965.87 us` | `1118.27 us` | `1279.49 us` |
| optional_match_string_filter | optional-match | `696.97 us` | `684.55 us` | `782.14 us` | `880.51 us` |
| optional_match_grouped_count | optional-match | `707.86 us` | `698.10 us` | `766.76 us` | `897.51 us` |
| with_scalar_rebinding | with-return | `557.09 us` | `550.32 us` | `594.10 us` | `723.37 us` |
| with_grouped_aggregate | aggregation | `869.85 us` | `861.36 us` | `923.09 us` | `1030.82 us` |
| variable_length_projection | variable-length | `940.31 us` | `928.70 us` | `1021.41 us` | `1135.04 us` |
| variable_length_grouped_count | variable-length | `998.75 us` | `984.66 us` | `1091.60 us` | `1219.88 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `926.79 us` | `912.25 us` | `1021.94 us` | `1135.99 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1042.97 us` | `1022.76 us` | `1164.87 us` | `1303.18 us` |
| searched_case_projection | computed-projection | `1056.20 us` | `1030.30 us` | `1190.23 us` | `1377.01 us` |
| graph_introspection | introspection | `1155.51 us` | `1140.67 us` | `1246.78 us` | `1390.28 us` |
| metadata_projection | metadata-projection | `1292.97 us` | `1280.44 us` | `1378.81 us` | `1461.80 us` |
| relationship_property_functions | computed-projection | `1246.33 us` | `1234.52 us` | `1319.52 us` | `1426.80 us` |
| unwind_literal_list | unwind | `675.63 us` | `665.64 us` | `740.78 us` | `806.02 us` |
| create_node_program | write-program | `335.28 us` | `332.72 us` | `351.34 us` | `399.50 us` |
| traversal_create_program | write-program | `679.58 us` | `671.97 us` | `722.95 us` | `844.24 us` |
| traversal_merge_program | write-program | `740.36 us` | `733.97 us` | `777.79 us` | `884.17 us` |
| match_set_node | write-single-statement | `464.96 us` | `458.34 us` | `511.79 us` | `588.67 us` |
| match_delete_relationship | write-single-statement | `411.64 us` | `403.72 us` | `467.45 us` | `554.86 us` |

### `duckdb` / `to_sqlglot_program` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `674.40 us` | `658.14 us` | `772.83 us` | `911.19 us` |
| relationship_projection | one-hop-read | `980.18 us` | `959.37 us` | `1091.72 us` | `1251.85 us` |
| optional_match_string_filter | optional-match | `691.22 us` | `680.15 us` | `762.82 us` | `864.26 us` |
| optional_match_grouped_count | optional-match | `701.86 us` | `694.68 us` | `745.98 us` | `828.83 us` |
| with_scalar_rebinding | with-return | `559.39 us` | `550.54 us` | `614.76 us` | `694.53 us` |
| with_grouped_aggregate | aggregation | `893.96 us` | `868.14 us` | `1031.68 us` | `1223.79 us` |
| variable_length_projection | variable-length | `964.79 us` | `942.99 us` | `1101.09 us` | `1312.39 us` |
| variable_length_grouped_count | variable-length | `1020.41 us` | `996.66 us` | `1147.66 us` | `1319.18 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `941.79 us` | `917.15 us` | `1073.25 us` | `1254.75 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1047.63 us` | `1023.05 us` | `1179.73 us` | `1344.97 us` |
| searched_case_projection | computed-projection | `1060.59 us` | `1035.77 us` | `1198.20 us` | `1344.43 us` |
| graph_introspection | introspection | `1174.32 us` | `1152.67 us` | `1297.15 us` | `1455.04 us` |
| metadata_projection | metadata-projection | `1294.53 us` | `1278.42 us` | `1394.75 us` | `1577.74 us` |
| relationship_property_functions | computed-projection | `1244.55 us` | `1234.59 us` | `1304.22 us` | `1457.85 us` |
| unwind_literal_list | unwind | `673.76 us` | `664.31 us` | `731.88 us` | `850.57 us` |
| create_node_program | write-program | `341.90 us` | `335.15 us` | `381.09 us` | `468.89 us` |
| traversal_create_program | write-program | `676.19 us` | `672.95 us` | `700.73 us` | `757.80 us` |
| traversal_merge_program | write-program | `742.34 us` | `737.46 us` | `774.46 us` | `843.37 us` |
| match_set_node | write-single-statement | `463.48 us` | `458.48 us` | `494.05 us` | `579.61 us` |
| match_delete_relationship | write-single-statement | `422.08 us` | `408.97 us` | `501.46 us` | `602.37 us` |

### `postgresql` / `to_sqlglot_program` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `667.77 us` | `660.16 us` | `722.50 us` | `796.31 us` |
| relationship_projection | one-hop-read | `995.65 us` | `969.92 us` | `1131.13 us` | `1310.97 us` |
| optional_match_string_filter | optional-match | `692.94 us` | `682.60 us` | `765.14 us` | `838.71 us` |
| optional_match_grouped_count | optional-match | `717.56 us` | `699.33 us` | `823.86 us` | `951.05 us` |
| with_scalar_rebinding | with-return | `572.42 us` | `555.01 us` | `672.12 us` | `804.04 us` |
| with_grouped_aggregate | aggregation | `876.66 us` | `862.92 us` | `965.46 us` | `1100.51 us` |
| variable_length_projection | variable-length | `957.49 us` | `938.09 us` | `1072.89 us` | `1221.95 us` |
| variable_length_grouped_count | variable-length | `1013.70 us` | `991.10 us` | `1136.75 us` | `1340.12 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `943.89 us` | `922.83 us` | `1057.91 us` | `1229.39 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1039.28 us` | `1022.13 us` | `1140.25 us` | `1252.98 us` |
| searched_case_projection | computed-projection | `1049.26 us` | `1029.92 us` | `1155.41 us` | `1297.71 us` |
| graph_introspection | introspection | `1157.47 us` | `1140.29 us` | `1257.36 us` | `1436.97 us` |
| metadata_projection | metadata-projection | `1294.98 us` | `1276.58 us` | `1403.75 us` | `1610.73 us` |
| relationship_property_functions | computed-projection | `1280.52 us` | `1250.09 us` | `1438.40 us` | `1712.49 us` |
| unwind_literal_list | unwind | `682.74 us` | `667.43 us` | `781.68 us` | `901.29 us` |
| create_node_program | write-program | `341.78 us` | `334.59 us` | `390.78 us` | `467.68 us` |
| traversal_create_program | write-program | `681.26 us` | `673.00 us` | `729.61 us` | `837.01 us` |
| traversal_merge_program | write-program | `753.32 us` | `737.40 us` | `843.74 us` | `1023.02 us` |
| match_set_node | write-single-statement | `465.07 us` | `457.54 us` | `510.02 us` | `608.66 us` |
| match_delete_relationship | write-single-statement | `415.63 us` | `406.29 us` | `476.91 us` | `574.69 us` |

### `sqlite` / `render_cypher_program_text` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `737.99 us` | `731.74 us` | `771.75 us` | `868.93 us` |
| relationship_projection | one-hop-read | `1095.35 us` | `1080.28 us` | `1193.11 us` | `1329.60 us` |
| optional_match_string_filter | optional-match | `812.54 us` | `794.08 us` | `917.74 us` | `1055.57 us` |
| optional_match_grouped_count | optional-match | `821.81 us` | `808.72 us` | `909.39 us` | `1006.43 us` |
| with_scalar_rebinding | with-return | `643.93 us` | `635.82 us` | `692.62 us` | `799.18 us` |
| with_grouped_aggregate | aggregation | `987.20 us` | `973.92 us` | `1070.78 us` | `1220.80 us` |
| variable_length_projection | variable-length | `1174.38 us` | `1165.51 us` | `1259.13 us` | `1444.13 us` |
| variable_length_grouped_count | variable-length | `1245.11 us` | `1232.45 us` | `1324.87 us` | `1466.80 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `1078.22 us` | `1058.69 us` | `1189.19 us` | `1325.37 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1282.25 us` | `1263.12 us` | `1383.41 us` | `1529.04 us` |
| searched_case_projection | computed-projection | `1150.59 us` | `1140.47 us` | `1223.78 us` | `1333.46 us` |
| graph_introspection | introspection | `1285.39 us` | `1265.43 us` | `1403.41 us` | `1563.12 us` |
| metadata_projection | metadata-projection | `1444.37 us` | `1418.53 us` | `1583.06 us` | `1764.40 us` |
| relationship_property_functions | computed-projection | `1424.89 us` | `1401.75 us` | `1552.67 us` | `1750.05 us` |
| unwind_literal_list | unwind | `770.36 us` | `754.19 us` | `867.97 us` | `1002.90 us` |
| create_node_program | write-program | `391.40 us` | `384.14 us` | `437.93 us` | `511.04 us` |
| traversal_create_program | write-program | `834.84 us` | `820.80 us` | `927.54 us` | `1044.67 us` |
| traversal_merge_program | write-program | `958.55 us` | `940.22 us` | `1061.57 us` | `1219.80 us` |
| match_set_node | write-single-statement | `516.32 us` | `503.42 us` | `598.22 us` | `681.05 us` |
| match_delete_relationship | write-single-statement | `510.90 us` | `501.39 us` | `573.00 us` | `679.93 us` |

### `duckdb` / `render_cypher_program_text` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `746.35 us` | `730.98 us` | `839.63 us` | `941.98 us` |
| relationship_projection | one-hop-read | `1104.97 us` | `1089.40 us` | `1211.80 us` | `1352.33 us` |
| optional_match_string_filter | optional-match | `797.06 us` | `785.31 us` | `872.92 us` | `966.17 us` |
| optional_match_grouped_count | optional-match | `817.39 us` | `797.71 us` | `916.18 us` | `1079.75 us` |
| with_scalar_rebinding | with-return | `635.83 us` | `626.30 us` | `698.82 us` | `803.30 us` |
| with_grouped_aggregate | aggregation | `976.65 us` | `963.32 us` | `1059.42 us` | `1197.25 us` |
| variable_length_projection | variable-length | `1150.20 us` | `1147.68 us` | `1196.82 us` | `1343.90 us` |
| variable_length_grouped_count | variable-length | `1224.55 us` | `1209.33 us` | `1300.69 us` | `1506.88 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `1059.73 us` | `1039.59 us` | `1185.92 us` | `1335.52 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1282.12 us` | `1261.58 us` | `1402.16 us` | `1543.82 us` |
| searched_case_projection | computed-projection | `1138.91 us` | `1126.75 us` | `1213.95 us` | `1368.37 us` |
| graph_introspection | introspection | `1247.64 us` | `1239.51 us` | `1288.35 us` | `1403.47 us` |
| metadata_projection | metadata-projection | `1390.92 us` | `1382.96 us` | `1431.08 us` | `1548.14 us` |
| relationship_property_functions | computed-projection | `1456.84 us` | `1434.73 us` | `1577.47 us` | `1794.45 us` |
| unwind_literal_list | unwind | `766.38 us` | `751.85 us` | `861.69 us` | `956.15 us` |
| create_node_program | write-program | `382.56 us` | `377.85 us` | `410.83 us` | `481.11 us` |
| traversal_create_program | write-program | `820.74 us` | `804.22 us` | `919.72 us` | `1047.26 us` |
| traversal_merge_program | write-program | `936.05 us` | `925.74 us` | `1003.19 us` | `1116.50 us` |
| match_set_node | write-single-statement | `503.59 us` | `496.35 us` | `553.33 us` | `647.28 us` |
| match_delete_relationship | write-single-statement | `501.06 us` | `495.13 us` | `544.48 us` | `603.16 us` |

### `postgresql` / `render_cypher_program_text` per-query

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `741.45 us` | `728.24 us` | `825.86 us` | `947.13 us` |
| relationship_projection | one-hop-read | `1087.47 us` | `1072.59 us` | `1174.64 us` | `1380.04 us` |
| optional_match_string_filter | optional-match | `786.38 us` | `780.91 us` | `813.55 us` | `904.09 us` |
| optional_match_grouped_count | optional-match | `800.15 us` | `793.39 us` | `839.09 us` | `943.77 us` |
| with_scalar_rebinding | with-return | `636.05 us` | `625.81 us` | `705.72 us` | `783.16 us` |
| with_grouped_aggregate | aggregation | `990.03 us` | `968.84 us` | `1108.44 us` | `1303.70 us` |
| variable_length_projection | variable-length | `1164.93 us` | `1151.28 us` | `1258.92 us` | `1426.20 us` |
| variable_length_grouped_count | variable-length | `1257.45 us` | `1229.91 us` | `1419.28 us` | `1647.15 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `1074.99 us` | `1054.34 us` | `1192.16 us` | `1355.72 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `1281.98 us` | `1261.49 us` | `1395.56 us` | `1532.70 us` |
| searched_case_projection | computed-projection | `1152.05 us` | `1134.48 us` | `1255.04 us` | `1399.07 us` |
| graph_introspection | introspection | `1278.88 us` | `1262.31 us` | `1380.39 us` | `1522.57 us` |
| metadata_projection | metadata-projection | `1416.12 us` | `1397.43 us` | `1523.13 us` | `1672.29 us` |
| relationship_property_functions | computed-projection | `1420.38 us` | `1398.32 us` | `1541.17 us` | `1709.35 us` |
| unwind_literal_list | unwind | `777.14 us` | `759.23 us` | `886.71 us` | `1011.32 us` |
| create_node_program | write-program | `389.44 us` | `383.85 us` | `427.30 us` | `495.64 us` |
| traversal_create_program | write-program | `818.08 us` | `810.96 us` | `862.31 us` | `954.24 us` |
| traversal_merge_program | write-program | `967.77 us` | `939.26 us` | `1119.23 us` | `1353.27 us` |
| match_set_node | write-single-statement | `497.82 us` | `494.98 us` | `518.91 us` | `570.41 us` |
| match_delete_relationship | write-single-statement | `497.49 us` | `494.06 us` | `521.18 us` | `575.92 us` |

## Backend Lowering

| Backend | Queries | Build IR P50 | Bind Backend P50 | Lower Backend P50 | Render Program P50 | End-to-End P50 | End-to-End P95 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| sqlite | 20 | `2.93 us` | `0.38 us` | `65.82 us` | `67.99 us` | `962.29 us` | `1440.54 us` |
| duckdb | 20 | `2.95 us` | `0.38 us` | `67.28 us` | `67.48 us` | `961.84 us` | `1458.87 us` |
| postgresql | 20 | `2.93 us` | `0.37 us` | `65.87 us` | `66.78 us` | `961.48 us` | `1419.33 us` |

## Backend Lowering Per Query

### `sqlite` backend lowering per-query

| Query | Category | Build IR P50 | Build IR P95 | Bind Backend P50 | Bind Backend P95 | Lower Backend P50 | Lower Backend P95 | Render Program P50 | Render Program P95 | End-to-End P50 | End-to-End P95 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `2.80 us` | `4.35 us` | `0.38 us` | `0.41 us` | `33.88 us` | `39.76 us` | `30.91 us` | `34.64 us` | `732.68 us` | `795.25 us` |
| relationship_projection | one-hop-read | `2.90 us` | `4.29 us` | `0.37 us` | `0.52 us` | `57.68 us` | `76.33 us` | `71.67 us` | `80.65 us` | `1080.89 us` | `1195.39 us` |
| optional_match_string_filter | optional-match | `2.86 us` | `2.93 us` | `0.37 us` | `0.40 us` | `62.73 us` | `75.27 us` | `56.88 us` | `62.12 us` | `791.78 us` | `847.61 us` |
| optional_match_grouped_count | optional-match | `2.85 us` | `2.95 us` | `0.38 us` | `0.40 us` | `56.12 us` | `64.39 us` | `52.97 us` | `58.11 us` | `800.28 us` | `825.55 us` |
| with_scalar_rebinding | with-return | `3.92 us` | `5.28 us` | `0.37 us` | `0.41 us` | `37.96 us` | `43.35 us` | `36.67 us` | `41.22 us` | `629.76 us` | `667.44 us` |
| with_grouped_aggregate | aggregation | `3.91 us` | `4.05 us` | `0.38 us` | `0.40 us` | `63.12 us` | `71.84 us` | `56.31 us` | `62.25 us` | `966.70 us` | `1022.38 us` |
| variable_length_projection | variable-length | `3.01 us` | `3.10 us` | `0.40 us` | `0.43 us` | `134.60 us` | `156.26 us` | `156.42 us` | `170.27 us` | `1160.93 us` | `1227.76 us` |
| variable_length_grouped_count | variable-length | `2.92 us` | `3.01 us` | `0.38 us` | `0.41 us` | `161.13 us` | `205.97 us` | `176.42 us` | `191.40 us` | `1226.53 us` | `1307.01 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `2.93 us` | `3.01 us` | `0.37 us` | `0.40 us` | `70.81 us` | `92.39 us` | `90.35 us` | `102.19 us` | `1043.84 us` | `1097.89 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `2.88 us` | `4.44 us` | `0.38 us` | `0.40 us` | `158.81 us` | `178.28 us` | `183.52 us` | `195.99 us` | `1260.66 us` | `1344.74 us` |
| searched_case_projection | computed-projection | `3.99 us` | `5.60 us` | `0.37 us` | `0.40 us` | `91.29 us` | `119.91 us` | `64.02 us` | `76.81 us` | `1144.57 us` | `1233.37 us` |
| graph_introspection | introspection | `2.90 us` | `2.98 us` | `0.39 us` | `0.41 us` | `65.08 us` | `89.11 us` | `70.95 us` | `86.50 us` | `1267.23 us` | `1431.85 us` |
| metadata_projection | metadata-projection | `2.95 us` | `3.02 us` | `0.38 us` | `0.41 us` | `72.19 us` | `90.68 us` | `79.76 us` | `94.21 us` | `1412.14 us` | `1560.61 us` |
| relationship_property_functions | computed-projection | `2.92 us` | `3.00 us` | `0.42 us` | `0.45 us` | `96.53 us` | `113.66 us` | `130.13 us` | `154.36 us` | `1448.60 us` | `1594.07 us` |
| unwind_literal_list | unwind | `2.99 us` | `3.13 us` | `0.38 us` | `0.40 us` | `57.33 us` | `73.75 us` | `49.00 us` | `75.54 us` | `763.40 us` | `882.47 us` |
| create_node_program | write-program | `1.99 us` | `2.76 us` | `0.38 us` | `0.41 us` | `32.75 us` | `38.81 us` | `19.69 us` | `26.75 us` | `384.20 us` | `461.75 us` |
| traversal_create_program | write-program | `3.57 us` | `3.66 us` | `0.38 us` | `0.41 us` | `94.41 us` | `123.00 us` | `77.62 us` | `84.77 us` | `818.63 us` | `927.43 us` |
| traversal_merge_program | write-program | `3.66 us` | `6.15 us` | `0.37 us` | `0.40 us` | `144.24 us` | `189.34 us` | `124.22 us` | `140.68 us` | `943.61 us` | `1076.61 us` |
| match_set_node | write-single-statement | `2.45 us` | `3.69 us` | `0.40 us` | `0.61 us` | `28.89 us` | `38.97 us` | `18.79 us` | `22.43 us` | `505.33 us` | `605.03 us` |
| match_delete_relationship | write-single-statement | `2.56 us` | `2.64 us` | `0.37 us` | `0.40 us` | `43.27 us` | `50.54 us` | `55.14 us` | `81.66 us` | `509.32 us` | `633.66 us` |

### `duckdb` backend lowering per-query

| Query | Category | Build IR P50 | Build IR P95 | Bind Backend P50 | Bind Backend P95 | Lower Backend P50 | Lower Backend P95 | Render Program P50 | Render Program P95 | End-to-End P50 | End-to-End P95 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `2.83 us` | `2.93 us` | `0.37 us` | `0.40 us` | `34.14 us` | `45.96 us` | `30.87 us` | `35.94 us` | `744.48 us` | `874.76 us` |
| relationship_projection | one-hop-read | `2.99 us` | `4.42 us` | `0.39 us` | `0.61 us` | `60.38 us` | `98.42 us` | `71.80 us` | `87.37 us` | `1101.65 us` | `1336.18 us` |
| optional_match_string_filter | optional-match | `2.89 us` | `3.48 us` | `0.38 us` | `0.45 us` | `63.91 us` | `95.21 us` | `57.22 us` | `71.66 us` | `796.92 us` | `934.97 us` |
| optional_match_grouped_count | optional-match | `2.89 us` | `3.46 us` | `0.38 us` | `0.44 us` | `57.34 us` | `89.21 us` | `52.79 us` | `61.28 us` | `805.74 us` | `927.10 us` |
| with_scalar_rebinding | with-return | `3.92 us` | `5.89 us` | `0.38 us` | `0.60 us` | `38.27 us` | `44.93 us` | `37.19 us` | `55.97 us` | `638.48 us` | `792.06 us` |
| with_grouped_aggregate | aggregation | `3.97 us` | `5.32 us` | `0.38 us` | `0.41 us` | `63.64 us` | `74.56 us` | `56.61 us` | `64.00 us` | `973.85 us` | `1092.71 us` |
| variable_length_projection | variable-length | `2.91 us` | `3.02 us` | `0.38 us` | `0.57 us` | `137.73 us` | `176.50 us` | `155.60 us` | `177.49 us` | `1174.87 us` | `1344.88 us` |
| variable_length_grouped_count | variable-length | `2.92 us` | `2.99 us` | `0.38 us` | `0.41 us` | `160.05 us` | `194.40 us` | `174.41 us` | `195.34 us` | `1234.39 us` | `1363.96 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `2.96 us` | `3.08 us` | `0.37 us` | `0.40 us` | `72.38 us` | `106.42 us` | `89.32 us` | `98.92 us` | `1061.10 us` | `1239.08 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `2.92 us` | `4.20 us` | `0.38 us` | `0.44 us` | `162.90 us` | `207.42 us` | `185.19 us` | `212.96 us` | `1290.86 us` | `1429.93 us` |
| searched_case_projection | computed-projection | `3.97 us` | `4.04 us` | `0.38 us` | `0.40 us` | `91.12 us` | `113.45 us` | `62.74 us` | `69.15 us` | `1147.42 us` | `1305.60 us` |
| graph_introspection | introspection | `2.99 us` | `4.10 us` | `0.37 us` | `0.40 us` | `64.47 us` | `83.33 us` | `71.05 us` | `83.90 us` | `1278.28 us` | `1452.89 us` |
| metadata_projection | metadata-projection | `2.93 us` | `3.05 us` | `0.38 us` | `0.42 us` | `73.58 us` | `107.19 us` | `79.96 us` | `90.94 us` | `1416.75 us` | `1604.39 us` |
| relationship_property_functions | computed-projection | `2.91 us` | `3.03 us` | `0.38 us` | `0.41 us` | `94.74 us` | `119.47 us` | `132.57 us` | `143.90 us` | `1465.14 us` | `1661.59 us` |
| unwind_literal_list | unwind | `3.02 us` | `4.58 us` | `0.38 us` | `0.42 us` | `57.06 us` | `69.10 us` | `47.67 us` | `55.17 us` | `760.47 us` | `839.25 us` |
| create_node_program | write-program | `1.87 us` | `1.95 us` | `0.38 us` | `0.40 us` | `32.48 us` | `36.23 us` | `19.78 us` | `21.93 us` | `379.31 us` | `401.94 us` |
| traversal_create_program | write-program | `3.56 us` | `3.75 us` | `0.38 us` | `0.52 us` | `94.22 us` | `106.66 us` | `77.62 us` | `85.31 us` | `808.90 us` | `847.00 us` |
| traversal_merge_program | write-program | `3.65 us` | `3.81 us` | `0.37 us` | `0.41 us` | `145.13 us` | `199.14 us` | `123.09 us` | `133.05 us` | `931.77 us` | `969.16 us` |
| match_set_node | write-single-statement | `2.37 us` | `2.49 us` | `0.38 us` | `0.41 us` | `28.70 us` | `32.64 us` | `18.64 us` | `22.88 us` | `499.24 us` | `550.53 us` |
| match_delete_relationship | write-single-statement | `2.56 us` | `4.01 us` | `0.38 us` | `0.60 us` | `42.84 us` | `53.58 us` | `54.04 us` | `59.89 us` | `503.08 us` | `569.18 us` |

### `postgresql` backend lowering per-query

| Query | Category | Build IR P50 | Build IR P95 | Bind Backend P50 | Bind Backend P95 | Lower Backend P50 | Lower Backend P95 | Render Program P50 | Render Program P95 | End-to-End P50 | End-to-End P95 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| simple_match_filter_limit | match-read | `2.83 us` | `2.97 us` | `0.37 us` | `0.40 us` | `34.49 us` | `39.21 us` | `31.42 us` | `36.10 us` | `737.92 us` | `788.50 us` |
| relationship_projection | one-hop-read | `2.92 us` | `3.01 us` | `0.37 us` | `0.40 us` | `58.50 us` | `67.88 us` | `71.17 us` | `88.08 us` | `1082.96 us` | `1221.68 us` |
| optional_match_string_filter | optional-match | `2.87 us` | `2.94 us` | `0.38 us` | `0.40 us` | `63.19 us` | `87.09 us` | `57.26 us` | `63.77 us` | `789.41 us` | `818.28 us` |
| optional_match_grouped_count | optional-match | `2.84 us` | `2.92 us` | `0.37 us` | `0.40 us` | `56.29 us` | `63.95 us` | `52.81 us` | `57.98 us` | `798.67 us` | `826.46 us` |
| with_scalar_rebinding | with-return | `3.97 us` | `4.05 us` | `0.37 us` | `0.40 us` | `38.30 us` | `44.51 us` | `36.78 us` | `40.97 us` | `628.82 us` | `655.69 us` |
| with_grouped_aggregate | aggregation | `3.94 us` | `4.08 us` | `0.37 us` | `0.52 us` | `62.93 us` | `86.63 us` | `56.84 us` | `62.79 us` | `972.44 us` | `1068.63 us` |
| variable_length_projection | variable-length | `2.90 us` | `2.98 us` | `0.38 us` | `0.41 us` | `134.26 us` | `154.47 us` | `155.22 us` | `172.23 us` | `1150.76 us` | `1235.85 us` |
| variable_length_grouped_count | variable-length | `2.92 us` | `3.02 us` | `0.38 us` | `0.41 us` | `160.52 us` | `192.15 us` | `175.54 us` | `203.05 us` | `1218.88 us` | `1257.85 us` |
| fixed_length_multi_hop_projection | fixed-length-multi-hop | `2.96 us` | `3.86 us` | `0.37 us` | `0.53 us` | `70.79 us` | `80.13 us` | `91.54 us` | `98.80 us` | `1050.23 us` | `1094.50 us` |
| zero_hop_variable_length_projection | variable-length-zero-hop | `2.96 us` | `4.20 us` | `0.38 us` | `0.51 us` | `159.93 us` | `182.31 us` | `183.77 us` | `199.80 us` | `1256.27 us` | `1331.76 us` |
| searched_case_projection | computed-projection | `3.97 us` | `4.09 us` | `0.37 us` | `0.40 us` | `88.61 us` | `111.26 us` | `62.25 us` | `70.15 us` | `1132.02 us` | `1268.32 us` |
| graph_introspection | introspection | `2.91 us` | `4.22 us` | `0.37 us` | `0.40 us` | `64.87 us` | `94.82 us` | `70.90 us` | `85.67 us` | `1283.36 us` | `1500.56 us` |
| metadata_projection | metadata-projection | `2.91 us` | `3.01 us` | `0.38 us` | `0.41 us` | `72.85 us` | `104.80 us` | `81.24 us` | `115.93 us` | `1420.13 us` | `1622.71 us` |
| relationship_property_functions | computed-projection | `2.91 us` | `3.08 us` | `0.38 us` | `0.40 us` | `95.11 us` | `129.99 us` | `100.45 us` | `114.57 us` | `1404.63 us` | `1540.18 us` |
| unwind_literal_list | unwind | `2.99 us` | `3.08 us` | `0.38 us` | `0.42 us` | `56.71 us` | `72.26 us` | `47.65 us` | `53.77 us` | `758.73 us` | `830.73 us` |
| create_node_program | write-program | `1.89 us` | `1.96 us` | `0.38 us` | `0.58 us` | `33.08 us` | `44.86 us` | `19.63 us` | `22.11 us` | `379.71 us` | `416.97 us` |
| traversal_create_program | write-program | `3.56 us` | `4.11 us` | `0.37 us` | `0.63 us` | `94.56 us` | `118.79 us` | `78.82 us` | `89.42 us` | `816.86 us` | `917.67 us` |
| traversal_merge_program | write-program | `3.61 us` | `3.76 us` | `0.37 us` | `0.40 us` | `146.88 us` | `181.49 us` | `125.94 us` | `136.43 us` | `938.29 us` | `995.06 us` |
| match_set_node | write-single-statement | `2.44 us` | `2.51 us` | `0.38 us` | `0.40 us` | `28.47 us` | `37.22 us` | `18.92 us` | `21.09 us` | `497.49 us` | `532.86 us` |
| match_delete_relationship | write-single-statement | `2.57 us` | `2.66 us` | `0.38 us` | `0.42 us` | `42.43 us` | `47.50 us` | `54.43 us` | `61.11 us` | `500.72 us` | `526.91 us` |


## SQLGlot Suites

| Implementation | Method | Dialects | Queries | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| compiled | tokenize | postgres -> sqlite | 22 | `13.83 us` | `12.26 us` | `26.31 us` | `31.97 us` |
| compiled | parse_one | postgres -> sqlite | 22 | `39.32 us` | `34.63 us` | `82.33 us` | `95.17 us` |
| compiled | parse_one_to_sql | postgres -> sqlite | 22 | `112.34 us` | `100.27 us` | `252.18 us` | `290.36 us` |
| compiled | transpile | postgres -> sqlite | 22 | `67.35 us` | `61.30 us` | `142.93 us` | `166.39 us` |
| python | tokenize | postgres -> sqlite | 22 | `54.52 us` | `45.58 us` | `148.63 us` | `167.35 us` |
| python | parse_one | postgres -> sqlite | 22 | `145.74 us` | `129.49 us` | `345.77 us` | `390.37 us` |
| python | parse_one_to_sql | postgres -> sqlite | 22 | `260.64 us` | `230.01 us` | `615.92 us` | `705.77 us` |
| python | transpile | postgres -> sqlite | 22 | `188.67 us` | `166.30 us` | `441.03 us` | `475.67 us` |

## SQLGlot Per Query

### SQLGlot `compiled` / `tokenize` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `9.75 us` | `9.42 us` | `10.15 us` | `17.63 us` |
| relationship_projection | join-read | `16.08 us` | `15.92 us` | `16.15 us` | `20.20 us` |
| left_join_fallback_projection | left-join | `13.64 us` | `13.57 us` | `13.75 us` | `16.69 us` |
| left_join_grouped_count | left-join | `14.35 us` | `14.28 us` | `14.45 us` | `17.80 us` |
| cte_scalar_rebinding | cte-return | `10.30 us` | `10.24 us` | `10.41 us` | `13.18 us` |
| grouped_aggregate | aggregation | `10.96 us` | `10.87 us` | `11.04 us` | `14.18 us` |
| recursive_projection | recursive-cte | `23.65 us` | `23.46 us` | `23.80 us` | `27.59 us` |
| recursive_grouped_count | recursive-cte | `26.10 us` | `25.98 us` | `26.35 us` | `29.73 us` |
| fixed_length_multi_hop_projection | join-read | `20.17 us` | `20.04 us` | `20.37 us` | `24.12 us` |
| recursive_zero_hop_projection | recursive-cte | `31.99 us` | `31.84 us` | `32.25 us` | `35.87 us` |
| searched_case_projection | computed-projection | `10.71 us` | `10.65 us` | `10.82 us` | `13.80 us` |
| id_projection | projection | `11.62 us` | `11.54 us` | `11.70 us` | `14.59 us` |
| relational_metadata_projection | projection | `15.65 us` | `15.52 us` | `15.77 us` | `19.61 us` |
| string_property_functions | computed-projection | `13.15 us` | `12.99 us` | `13.27 us` | `18.78 us` |
| values_projection | values | `10.21 us` | `10.08 us` | `10.24 us` | `14.94 us` |
| insert_user | insert | `7.42 us` | `7.37 us` | `7.50 us` | `10.23 us` |
| insert_edge_from_select | insert-select | `12.43 us` | `12.35 us` | `12.52 us` | `16.03 us` |
| insert_edge_with_conflict_ignore | insert-select | `13.51 us` | `13.43 us` | `13.61 us` | `16.60 us` |
| update_user | update | `7.28 us` | `7.19 us` | `7.32 us` | `10.79 us` |
| delete_relationship | delete | `6.76 us` | `6.70 us` | `6.82 us` | `9.11 us` |
| postgres_cast_projection | cast | `10.15 us` | `10.06 us` | `10.22 us` | `13.15 us` |
| relational_text_projection | computed-projection | `8.32 us` | `8.27 us` | `8.40 us` | `10.09 us` |

### SQLGlot `compiled` / `parse_one` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `27.79 us` | `27.43 us` | `31.07 us` | `36.58 us` |
| relationship_projection | join-read | `46.19 us` | `45.79 us` | `51.85 us` | `57.36 us` |
| left_join_fallback_projection | left-join | `43.13 us` | `42.21 us` | `49.48 us` | `67.68 us` |
| left_join_grouped_count | left-join | `44.35 us` | `43.33 us` | `50.54 us` | `75.64 us` |
| cte_scalar_rebinding | cte-return | `33.01 us` | `31.71 us` | `40.76 us` | `63.75 us` |
| grouped_aggregate | aggregation | `34.72 us` | `34.20 us` | `39.41 us` | `45.66 us` |
| recursive_projection | recursive-cte | `65.71 us` | `64.61 us` | `74.84 us` | `98.45 us` |
| recursive_grouped_count | recursive-cte | `79.77 us` | `78.10 us` | `91.31 us` | `115.74 us` |
| fixed_length_multi_hop_projection | join-read | `55.97 us` | `54.89 us` | `64.72 us` | `76.68 us` |
| recursive_zero_hop_projection | recursive-cte | `89.31 us` | `87.37 us` | `104.00 us` | `134.14 us` |
| searched_case_projection | computed-projection | `28.67 us` | `28.23 us` | `32.24 us` | `38.94 us` |
| id_projection | projection | `28.78 us` | `28.57 us` | `32.52 us` | `34.99 us` |
| relational_metadata_projection | projection | `42.71 us` | `41.36 us` | `50.91 us` | `76.94 us` |
| string_property_functions | computed-projection | `41.18 us` | `40.36 us` | `47.10 us` | `62.72 us` |
| values_projection | values | `28.28 us` | `27.88 us` | `32.01 us` | `40.34 us` |
| insert_user | insert | `17.74 us` | `17.33 us` | `20.69 us` | `27.22 us` |
| insert_edge_from_select | insert-select | `35.44 us` | `34.84 us` | `40.61 us` | `53.28 us` |
| insert_edge_with_conflict_ignore | insert-select | `37.93 us` | `36.70 us` | `45.38 us` | `72.50 us` |
| update_user | update | `18.52 us` | `18.06 us` | `22.28 us` | `29.70 us` |
| delete_relationship | delete | `14.74 us` | `13.89 us` | `21.71 us` | `28.78 us` |
| postgres_cast_projection | cast | `29.02 us` | `28.64 us` | `32.65 us` | `38.34 us` |
| relational_text_projection | computed-projection | `22.04 us` | `21.66 us` | `24.68 us` | `30.36 us` |

### SQLGlot `compiled` / `parse_one_to_sql` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `76.27 us` | `73.18 us` | `97.21 us` | `140.08 us` |
| relationship_projection | join-read | `133.05 us` | `129.76 us` | `157.82 us` | `211.90 us` |
| left_join_fallback_projection | left-join | `120.85 us` | `119.02 us` | `131.97 us` | `161.93 us` |
| left_join_grouped_count | left-join | `126.27 us` | `122.69 us` | `147.84 us` | `202.49 us` |
| cte_scalar_rebinding | cte-return | `82.00 us` | `81.33 us` | `88.95 us` | `94.12 us` |
| grouped_aggregate | aggregation | `96.11 us` | `92.98 us` | `118.46 us` | `157.99 us` |
| recursive_projection | recursive-cte | `199.42 us` | `188.89 us` | `270.19 us` | `342.29 us` |
| recursive_grouped_count | recursive-cte | `244.20 us` | `232.47 us` | `319.15 us` | `394.91 us` |
| fixed_length_multi_hop_projection | join-read | `171.47 us` | `167.23 us` | `203.18 us` | `259.98 us` |
| recursive_zero_hop_projection | recursive-cte | `268.22 us` | `262.70 us` | `311.89 us` | `381.75 us` |
| searched_case_projection | computed-projection | `76.02 us` | `74.56 us` | `84.51 us` | `105.01 us` |
| id_projection | projection | `83.58 us` | `81.28 us` | `95.91 us` | `137.97 us` |
| relational_metadata_projection | projection | `121.47 us` | `119.30 us` | `135.25 us` | `174.48 us` |
| string_property_functions | computed-projection | `122.97 us` | `120.11 us` | `140.27 us` | `180.42 us` |
| values_projection | values | `71.93 us` | `71.16 us` | `78.82 us` | `85.09 us` |
| insert_user | insert | `43.72 us` | `42.83 us` | `50.05 us` | `61.95 us` |
| insert_edge_from_select | insert-select | `106.07 us` | `102.94 us` | `124.15 us` | `172.89 us` |
| insert_edge_with_conflict_ignore | insert-select | `106.32 us` | `105.38 us` | `115.27 us` | `123.66 us` |
| update_user | update | `42.78 us` | `41.43 us` | `50.49 us` | `75.06 us` |
| delete_relationship | delete | `29.68 us` | `28.85 us` | `34.90 us` | `47.70 us` |
| postgres_cast_projection | cast | `89.37 us` | `86.93 us` | `100.66 us` | `147.52 us` |
| relational_text_projection | computed-projection | `59.78 us` | `58.80 us` | `67.40 us` | `77.07 us` |

### SQLGlot `compiled` / `transpile` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `45.71 us` | `45.13 us` | `50.93 us` | `56.88 us` |
| relationship_projection | join-read | `72.43 us` | `71.61 us` | `79.77 us` | `89.33 us` |
| left_join_fallback_projection | left-join | `72.71 us` | `72.03 us` | `80.18 us` | `85.05 us` |
| left_join_grouped_count | left-join | `77.63 us` | `75.47 us` | `88.47 us` | `132.70 us` |
| cte_scalar_rebinding | cte-return | `57.18 us` | `53.40 us` | `81.65 us` | `110.34 us` |
| grouped_aggregate | aggregation | `61.03 us` | `58.71 us` | `73.83 us` | `105.58 us` |
| recursive_projection | recursive-cte | `110.89 us` | `107.12 us` | `135.41 us` | `173.10 us` |
| recursive_grouped_count | recursive-cte | `139.95 us` | `135.00 us` | `169.93 us` | `213.24 us` |
| fixed_length_multi_hop_projection | join-read | `94.39 us` | `89.80 us` | `126.58 us` | `166.59 us` |
| recursive_zero_hop_projection | recursive-cte | `154.51 us` | `148.10 us` | `198.31 us` | `248.24 us` |
| searched_case_projection | computed-projection | `48.94 us` | `47.09 us` | `60.29 us` | `84.19 us` |
| id_projection | projection | `47.77 us` | `45.48 us` | `62.14 us` | `93.48 us` |
| relational_metadata_projection | projection | `69.43 us` | `66.90 us` | `86.85 us` | `120.69 us` |
| string_property_functions | computed-projection | `79.19 us` | `76.34 us` | `92.03 us` | `145.50 us` |
| values_projection | values | `45.30 us` | `44.92 us` | `49.74 us` | `54.48 us` |
| insert_user | insert | `28.24 us` | `27.90 us` | `31.23 us` | `37.83 us` |
| insert_edge_from_select | insert-select | `63.18 us` | `61.03 us` | `73.82 us` | `111.26 us` |
| insert_edge_with_conflict_ignore | insert-select | `64.04 us` | `62.65 us` | `72.08 us` | `94.18 us` |
| update_user | update | `28.09 us` | `26.79 us` | `37.79 us` | `50.54 us` |
| delete_relationship | delete | `21.15 us` | `20.52 us` | `24.98 us` | `36.09 us` |
| postgres_cast_projection | cast | `62.10 us` | `60.27 us` | `73.30 us` | `95.53 us` |
| relational_text_projection | computed-projection | `37.91 us` | `36.49 us` | `47.01 us` | `67.50 us` |

### SQLGlot `python` / `tokenize` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `30.49 us` | `29.03 us` | `39.34 us` | `50.91 us` |
| relationship_projection | join-read | `68.29 us` | `65.08 us` | `89.53 us` | `104.13 us` |
| left_join_fallback_projection | left-join | `53.10 us` | `50.95 us` | `69.81 us` | `81.53 us` |
| left_join_grouped_count | left-join | `56.20 us` | `53.24 us` | `76.72 us` | `94.62 us` |
| cte_scalar_rebinding | cte-return | `33.10 us` | `32.46 us` | `36.76 us` | `45.40 us` |
| grouped_aggregate | aggregation | `36.71 us` | `35.58 us` | `42.51 us` | `55.13 us` |
| recursive_projection | recursive-cte | `109.62 us` | `105.65 us` | `133.55 us` | `166.01 us` |
| recursive_grouped_count | recursive-cte | `126.93 us` | `122.57 us` | `154.73 us` | `183.86 us` |
| fixed_length_multi_hop_projection | join-read | `88.91 us` | `84.54 us` | `117.55 us` | `137.66 us` |
| recursive_zero_hop_projection | recursive-cte | `161.99 us` | `155.50 us` | `200.27 us` | `238.39 us` |
| searched_case_projection | computed-projection | `35.59 us` | `34.49 us` | `40.95 us` | `55.61 us` |
| id_projection | projection | `42.69 us` | `40.81 us` | `54.23 us` | `69.98 us` |
| relational_metadata_projection | projection | `62.46 us` | `60.21 us` | `74.51 us` | `94.95 us` |
| string_property_functions | computed-projection | `49.13 us` | `48.66 us` | `52.76 us` | `55.60 us` |
| values_projection | values | `33.49 us` | `32.65 us` | `38.07 us` | `50.17 us` |
| insert_user | insert | `19.69 us` | `19.33 us` | `21.80 us` | `28.51 us` |
| insert_edge_from_select | insert-select | `47.72 us` | `45.96 us` | `57.72 us` | `74.65 us` |
| insert_edge_with_conflict_ignore | insert-select | `52.02 us` | `50.16 us` | `62.92 us` | `80.59 us` |
| update_user | update | `18.29 us` | `17.58 us` | `22.88 us` | `29.31 us` |
| delete_relationship | delete | `15.61 us` | `15.10 us` | `18.70 us` | `25.41 us` |
| postgres_cast_projection | cast | `33.17 us` | `31.72 us` | `41.15 us` | `54.28 us` |
| relational_text_projection | computed-projection | `24.22 us` | `23.21 us` | `31.98 us` | `38.42 us` |

### SQLGlot `python` / `parse_one` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `96.26 us` | `91.74 us` | `124.80 us` | `161.10 us` |
| relationship_projection | join-read | `181.29 us` | `172.34 us` | `237.55 us` | `294.14 us` |
| left_join_fallback_projection | left-join | `158.01 us` | `150.06 us` | `214.42 us` | `262.28 us` |
| left_join_grouped_count | left-join | `161.22 us` | `151.49 us` | `221.07 us` | `275.50 us` |
| cte_scalar_rebinding | cte-return | `119.90 us` | `113.45 us` | `163.21 us` | `196.04 us` |
| grouped_aggregate | aggregation | `116.75 us` | `109.89 us` | `161.55 us` | `201.60 us` |
| recursive_projection | recursive-cte | `274.28 us` | `261.15 us` | `353.01 us` | `425.90 us` |
| recursive_grouped_count | recursive-cte | `314.29 us` | `297.48 us` | `409.02 us` | `496.82 us` |
| fixed_length_multi_hop_projection | join-read | `215.10 us` | `208.85 us` | `256.23 us` | `325.90 us` |
| recursive_zero_hop_projection | recursive-cte | `367.97 us` | `356.67 us` | `446.39 us` | `551.35 us` |
| searched_case_projection | computed-projection | `104.48 us` | `98.63 us` | `142.90 us` | `178.01 us` |
| id_projection | projection | `103.50 us` | `98.49 us` | `138.49 us` | `177.29 us` |
| relational_metadata_projection | projection | `151.14 us` | `145.84 us` | `185.68 us` | `238.89 us` |
| string_property_functions | computed-projection | `152.93 us` | `142.29 us` | `216.18 us` | `262.55 us` |
| values_projection | values | `98.07 us` | `93.04 us` | `132.49 us` | `166.77 us` |
| insert_user | insert | `55.23 us` | `53.30 us` | `64.82 us` | `93.01 us` |
| insert_edge_from_select | insert-select | `137.23 us` | `129.47 us` | `188.45 us` | `226.43 us` |
| insert_edge_with_conflict_ignore | insert-select | `147.05 us` | `137.12 us` | `207.06 us` | `253.22 us` |
| update_user | update | `52.34 us` | `50.59 us` | `60.34 us` | `89.22 us` |
| delete_relationship | delete | `37.67 us` | `36.39 us` | `43.16 us` | `62.45 us` |
| postgres_cast_projection | cast | `92.91 us` | `89.16 us` | `115.11 us` | `155.54 us` |
| relational_text_projection | computed-projection | `68.56 us` | `65.98 us` | `82.64 us` | `116.43 us` |

### SQLGlot `python` / `parse_one_to_sql` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `175.62 us` | `166.69 us` | `238.98 us` | `286.34 us` |
| relationship_projection | join-read | `324.52 us` | `311.54 us` | `410.72 us` | `502.60 us` |
| left_join_fallback_projection | left-join | `282.88 us` | `268.26 us` | `371.06 us` | `457.10 us` |
| left_join_grouped_count | left-join | `278.56 us` | `266.81 us` | `351.39 us` | `416.16 us` |
| cte_scalar_rebinding | cte-return | `208.68 us` | `197.42 us` | `276.43 us` | `335.93 us` |
| grouped_aggregate | aggregation | `202.40 us` | `191.90 us` | `269.16 us` | `326.67 us` |
| recursive_projection | recursive-cte | `486.29 us` | `465.62 us` | `610.14 us` | `721.47 us` |
| recursive_grouped_count | recursive-cte | `564.35 us` | `541.97 us` | `698.34 us` | `808.99 us` |
| fixed_length_multi_hop_projection | join-read | `387.32 us` | `378.04 us` | `446.43 us` | `533.52 us` |
| recursive_zero_hop_projection | recursive-cte | `663.65 us` | `643.43 us` | `786.60 us` | `914.25 us` |
| searched_case_projection | computed-projection | `173.16 us` | `167.16 us` | `210.63 us` | `267.22 us` |
| id_projection | projection | `192.31 us` | `182.79 us` | `249.60 us` | `307.54 us` |
| relational_metadata_projection | projection | `281.89 us` | `271.69 us` | `347.39 us` | `424.60 us` |
| string_property_functions | computed-projection | `269.18 us` | `259.97 us` | `326.60 us` | `406.54 us` |
| values_projection | values | `170.44 us` | `163.01 us` | `221.56 us` | `269.55 us` |
| insert_user | insert | `100.95 us` | `96.56 us` | `130.50 us` | `162.33 us` |
| insert_edge_from_select | insert-select | `241.18 us` | `229.89 us` | `315.83 us` | `382.18 us` |
| insert_edge_with_conflict_ignore | insert-select | `256.03 us` | `243.77 us` | `329.81 us` | `388.81 us` |
| update_user | update | `92.70 us` | `88.90 us` | `115.89 us` | `153.84 us` |
| delete_relationship | delete | `65.60 us` | `62.71 us` | `79.53 us` | `117.90 us` |
| postgres_cast_projection | cast | `185.87 us` | `179.17 us` | `230.74 us` | `293.46 us` |
| relational_text_projection | computed-projection | `130.42 us` | `123.57 us` | `178.01 us` | `216.80 us` |

### SQLGlot `python` / `transpile` per-query

- Dialects: `postgres -> sqlite`

| Query | Category | Mean | P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| simple_filter_limit | select-read | `132.78 us` | `124.22 us` | `188.51 us` | `233.58 us` |
| relationship_projection | join-read | `237.85 us` | `227.21 us` | `308.47 us` | `371.53 us` |
| left_join_fallback_projection | left-join | `204.78 us` | `198.00 us` | `249.77 us` | `306.02 us` |
| left_join_grouped_count | left-join | `205.10 us` | `197.20 us` | `253.27 us` | `324.39 us` |
| cte_scalar_rebinding | cte-return | `159.51 us` | `154.10 us` | `196.12 us` | `256.06 us` |
| grouped_aggregate | aggregation | `146.46 us` | `141.87 us` | `172.20 us` | `224.60 us` |
| recursive_projection | recursive-cte | `354.04 us` | `345.21 us` | `411.88 us` | `491.59 us` |
| recursive_grouped_count | recursive-cte | `402.16 us` | `394.67 us` | `453.68 us` | `539.96 us` |
| fixed_length_multi_hop_projection | join-read | `280.76 us` | `275.99 us` | `310.31 us` | `385.91 us` |
| recursive_zero_hop_projection | recursive-cte | `469.14 us` | `464.01 us` | `498.38 us` | `596.04 us` |
| searched_case_projection | computed-projection | `126.00 us` | `125.00 us` | `132.55 us` | `142.52 us` |
| id_projection | projection | `127.43 us` | `126.95 us` | `132.93 us` | `137.04 us` |
| relational_metadata_projection | projection | `198.42 us` | `193.39 us` | `229.96 us` | `286.11 us` |
| string_property_functions | computed-projection | `198.63 us` | `197.50 us` | `206.71 us` | `217.32 us` |
| values_projection | values | `122.73 us` | `119.43 us` | `136.30 us` | `195.09 us` |
| insert_user | insert | `74.45 us` | `72.70 us` | `82.66 us` | `110.10 us` |
| insert_edge_from_select | insert-select | `170.05 us` | `167.99 us` | `181.57 us` | `210.97 us` |
| insert_edge_with_conflict_ignore | insert-select | `184.33 us` | `178.06 us` | `222.01 us` | `267.38 us` |
| update_user | update | `71.69 us` | `67.99 us` | `94.09 us` | `123.34 us` |
| delete_relationship | delete | `48.86 us` | `47.36 us` | `54.91 us` | `80.39 us` |
| postgres_cast_projection | cast | `142.60 us` | `140.89 us` | `151.28 us` | `184.80 us` |
| relational_text_projection | computed-projection | `92.90 us` | `91.95 us` | `98.33 us` | `124.70 us` |


## SQLGlot Details

### SQLGlot `compiled` details

- Version: `30.6.0`
- Module files:
  - `sqlglot`: `/mnt/ssd2/repos/cypherglot/.venv/lib/python3.12/site-packages/sqlglot/__init__.py`
  - `parser`: `/mnt/ssd2/repos/cypherglot/.venv/lib/python3.12/site-packages/sqlglot/parser.cpython-312-x86_64-linux-gnu.so`
  - `generator`: `/mnt/ssd2/repos/cypherglot/.venv/lib/python3.12/site-packages/sqlglot/generator.cpython-312-x86_64-linux-gnu.so`
  - `tokenizer_core`: `/mnt/ssd2/repos/cypherglot/.venv/lib/python3.12/site-packages/sqlglot/tokenizer_core.cpython-312-x86_64-linux-gnu.so`

### SQLGlot `python` details

- Version: `30.6.0`
- Module files:
  - `sqlglot`: `/tmp/sqlglot-pure-sfhb2f2f/sqlglot/__init__.py`
  - `parser`: `/tmp/sqlglot-pure-sfhb2f2f/sqlglot/parser.py`
  - `generator`: `/tmp/sqlglot-pure-sfhb2f2f/sqlglot/generator.py`
  - `tokenizer_core`: `/tmp/sqlglot-pure-sfhb2f2f/sqlglot/tokenizer_core.py`

