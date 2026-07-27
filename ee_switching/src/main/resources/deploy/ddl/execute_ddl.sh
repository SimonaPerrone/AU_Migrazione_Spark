#!/usr/bin/env bash
set -e

SWITCHING_DB=${hive.db}

hive \
    -hiveconf switching_db=${SWITCHING_DB} \
    -f db-all.hql

for table_file in $(ls table-*.hql); do
    hive \
        -hiveconf switching_db=${SWITCHING_DB} \
        -f "${table_file}"
done