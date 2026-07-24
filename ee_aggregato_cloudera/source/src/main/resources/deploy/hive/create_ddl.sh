#!/bin/bash

hive -hiveconf hive_db=${hive.au} -f "${deploy.path}/hive/create/create_table_report_ammissibilita_file.hql"
hive -hiveconf hive_db=${hive.au} -f "${deploy.path}/hive/create/create_table_report_ammissibilita_pod.hql"
hive -hiveconf hive_db=${hive.au} -f "${deploy.path}/hive/create/create_table_report_ammissibilita_file_ee_tracking.hql"
hive -hiveconf hive_db=${hive.au} -f "${deploy.path}/hive/create/create_table_report_ammissibilita_pod_tracking.hql"
hive -hiveconf hive_db=${hive.au} -f "${deploy.path}/hive/create/create_table_report_ammissibilita_file_ee_tracking_staging.hql"
hive -hiveconf hive_db=${hive.au} -f "${deploy.path}/hive/create/create_table_report_ammissibilita_pod_tracking_staging.hql"
