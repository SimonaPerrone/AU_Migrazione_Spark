set hive.support.quoted.identifiers=none;

INSERT INTO eng_test.calcolo_consumi_sbg PARTITION(annomese_rif="202001")
select `(annomese_rif)?+.+` from prova.calcolo_consumi_sbg where annomese_rif="202001";


show partitions eng_test.calcolo_consumi_sbg;

show create table eng_test.calcolo_consumi_sbg;

ALTER TABLE eng_test.calcolo_consumi_sbg DROP PARTITION (annomese_rif="202001") PURGE;
hdfs dfs -ls /user/hive/warehouse/eng_test.db/calcolo_consumi_sbg/
hdfs dfs -rm -R -skipTrash /user/hive/warehouse/eng_test.db/calcolo_consumi_sbg/annomese_rif=202109