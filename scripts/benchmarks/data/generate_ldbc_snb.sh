#!/usr/bin/env bash
# Generate an LDBC SNB dataset for the CypherGlot second-topology benchmark.
#
# Produces the Datagen "bi / csv / composite-merged-fk" layout that
# scripts/benchmarks/common/topology_ldbc_snb.py consumes. Point the benchmark
# at the output directory with:
#
#   --topology ldbc_snb --ldbc-snb-data-dir <OUTPUT_DIR>
#
# The loader reads only the initial_snapshot (a self-consistent static graph);
# the temporal insert/delete batches are ignored by design and declared in the
# paper's methodology.
#
# Usage:
#   scripts/benchmarks/data/generate_ldbc_snb.sh <SCALE_FACTOR> <OUTPUT_DIR> [PARALLELISM]
#
# Examples (the paper's two scale points):
#   scripts/benchmarks/data/generate_ldbc_snb.sh 1  /data/ldbc_snb/sf1
#   scripts/benchmarks/data/generate_ldbc_snb.sh 10 /data/ldbc_snb/sf10 8
#
# Requires Docker. SF1 is a few hundred MB and generates in ~minutes on a
# laptop; SF10 (~30M nodes) is the >=10M headline scale and is generated on the
# measurement host (mini). Pin the image tag so the dataset is reproducible.
set -euo pipefail

DATAGEN_IMAGE="ldbc/datagen-standalone:0.5.1-2.12_spark3.2"

SCALE_FACTOR="${1:?usage: generate_ldbc_snb.sh <SCALE_FACTOR> <OUTPUT_DIR> [PARALLELISM]}"
OUTPUT_DIR="${2:?usage: generate_ldbc_snb.sh <SCALE_FACTOR> <OUTPUT_DIR> [PARALLELISM]}"
PARALLELISM="${3:-1}"

mkdir -p "$OUTPUT_DIR"
# The container writes as its own user; make the bind mount writable.
chmod 777 "$OUTPUT_DIR"

echo "Generating LDBC SNB SF${SCALE_FACTOR} into ${OUTPUT_DIR} (parallelism=${PARALLELISM})"
echo "Image: ${DATAGEN_IMAGE}"

docker run --rm \
  --mount type=bind,source="$(cd "$OUTPUT_DIR" && pwd)",target=/out \
  "$DATAGEN_IMAGE" \
  --parallelism "$PARALLELISM" \
  -- \
  --format csv \
  --scale-factor "$SCALE_FACTOR" \
  --mode bi \
  --output-dir /out

SNAPSHOT="$OUTPUT_DIR/graphs/csv/bi/composite-merged-fk/initial_snapshot"
if [[ -d "$SNAPSHOT/dynamic/Person" ]]; then
  echo "Done. Initial snapshot at: $SNAPSHOT"
  echo "Run with: --topology ldbc_snb --ldbc-snb-data-dir $OUTPUT_DIR"
else
  echo "WARNING: expected initial_snapshot not found under $SNAPSHOT" >&2
  exit 1
fi
