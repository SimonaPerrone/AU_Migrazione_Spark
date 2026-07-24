#execute_sqoop_import(){
#
#  local schema_in="$1"
#  local table_in="$2"
#  local schema_out="$3"
#  local table_out="$4"
#
# sqoop import -m 1 \
#    --connect "${JDBC_URL}" \
#    --username "${JDBC_USERNAME}" \
#    --password "${JDBC_PASSWORD}" \
#    --table "${schema_in}.${table_in}" \
#    --hive-import --create-hive-table \
#    --hive-table "${schema_out}.${table_out}"
#}
execute_sqoop_import()
{
    local schema="$1"
    local table_in="$2"
	  local target_dir="$3"
    sqoop import -m 1 \
        --connect "${JDBC_URL}" \
        --username "${JDBC_USERNAME}" \
        --password "${JDBC_PASSWORD}" \
        --table "${schema}.${table_in}" \
        --fields-terminated-by '|' \
        --target-dir "${target_dir}" \
        --lines-terminated-by '\n' \
        --null-string 'NULL' \
        --null-non-string 'NULL' \
        --delete-target-dir \
        --verbose
}
#    --staging-table "${table_out}_STAGING" \
#    --clear-staging-table \
execute_sqoop_export(){
  local db_in="$1"
  local table_in="$2"
  local db_out="$3"
  local table_out="$4"
  local col="$5"

  sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${db_out}.${table_out}" \
    --columns $col \
    --hcatalog-database "${db_in}" \
    --hcatalog-table "${table_in}" \
    --num-mappers ${sqoop.num.mappers}
}
load_jdbc(){
  # load bash JDBC variables from Java properties file in HDFS
source <(${sqoop.cat.command} '${sqoop.properties.hdfs.path}' \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )
}
execute_sqoop_query()
{
    local query="$1"
    sqoop eval --verbose --connect "${JDBC_URL}" --username "${JDBC_USERNAME}" --password "${JDBC_PASSWORD}" --query "${query}"
}