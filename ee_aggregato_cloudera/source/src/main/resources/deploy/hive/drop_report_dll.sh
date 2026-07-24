#!/bin/bash

hive -hiveconf hive_db=${hive.au} -f "${deploy.path}/hive/drop/drop_report.hql"