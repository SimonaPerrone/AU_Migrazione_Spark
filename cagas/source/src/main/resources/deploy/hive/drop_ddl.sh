

hive -e "DROP TABLE IF EXISTS ${hive.db}.validated_flows;"
hive -e "DROP TABLE IF EXISTS ${hive.db}.consumptions;"
hive -e "DROP TABLE IF EXISTS ${hive.db}.ca_final_to_export;"
hive -e "DROP TABLE IF EXISTS ${hive.db}.ca_pre_final;"
hive -e "DROP TABLE IF EXISTS ${hive.db}.ca_final;"
hive -e "DROP TABLE IF EXISTS ${hive.db}.ca;"