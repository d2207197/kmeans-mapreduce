#!/usr/bin/env bash

K=5
INPUT="$1"
CENTROIDS_FILE="$2"
OUTPUT_PREFIX="$3"



rm -rf ${OUTPUT_PREFIX}.*

trap 'exit' SIGHUP SIGINT SIGTERM

pv "$INPUT" | lmr 1m "$K" "./kmeans_mr.py -m --centroids-file \"${CENTROIDS_FILE}\"" './kmeans_mr.py -r' "${OUTPUT_PREFIX}.0"
cat "${OUTPUT_PREFIX}.0"/* | sort -k1,1 -t$'\t' -s > "$OUTPUT_PREFIX.centroids.0"

for i in $(seq 1 20)
do
    pv "$INPUT" | lmr 1m "$K" "./kmeans_mr.py -m --centroids-file $OUTPUT_PREFIX.centroids.$((i-1))" './kmeans_mr.py -r' "$OUTPUT_PREFIX.$i" || exit 1
    cat "$OUTPUT_PREFIX.$i"/* | sort -k1,1 -t$'\t' -s > "$OUTPUT_PREFIX.centroids.$i"
done

