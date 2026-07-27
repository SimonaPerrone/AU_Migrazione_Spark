
hive -f ${oozie.deploy.path}/DDL/DDL_ca.sql
hive -f ${oozie.deploy.path}/DDL/DDL_ca_pre_final.sql
hive -f ${oozie.deploy.path}/DDL/DDL_ca_final.sql
hive -f ${oozie.deploy.path}/DDL/DDL_consumptions.sql
hive -f ${oozie.deploy.path}/DDL/DDL_validations.sql
hive -f ${oozie.deploy.path}/DDL/DDL_ca_final_to_export.sql

hive -e "use ${hive.db}; msck repair table validated_flows;"
hive -e "use ${hive.db}; msck repair table consumptions;"
hive -e "use ${hive.db}; msck repair table ca;"
hive -e "use ${hive.db}; msck repair table ca_pre_final;"
hive -e "use ${hive.db}; msck repair table ca_final;"
hive -e "use ${hive.db}; msck repair table ca_final_to_export;"
