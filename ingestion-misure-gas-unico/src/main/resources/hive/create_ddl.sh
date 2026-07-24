
cd create

hive -hiveconf hive_db=${hive.cmg_gas} -f a01r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f a02.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f a02r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f a40r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad2.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad2r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad4r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad5.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad5r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f create_table_report_ammissibilita_file.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f create_table_report_ammissibilita_pdr.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f report_ingestion.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad3.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad3r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f ad4.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f d02r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f igmg.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f m01.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f d01r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f d02.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f r01r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f r40.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f r40r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f m01r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f r01.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f s40.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f s40r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f sm1r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f s02.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f s02r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f v01.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f v01r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f v02.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f v02r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f sm2.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f sm2r.hql
hive -hiveconf hive_db=${hive.cmg_gas} -f igmg_export.hql

