export CREATE_HISTORY=$(cat <<-EOF

set hive.support.quoted.identifiers=none;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO ${hive.table.result.db}.${hive.table.result.name}_history partition (n_execution_id)
select \`(n_execution_id)?+.+\`,n_execution_id from ${hive.table.result.db}.${hive.table.result.name};

EOF
)