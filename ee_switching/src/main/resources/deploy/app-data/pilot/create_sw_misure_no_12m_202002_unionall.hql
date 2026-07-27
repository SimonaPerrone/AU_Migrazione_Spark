MAPREDUCEPARAM-FILTER

---TODO: aggiungere un ulteriore ranking per i flussi doppi tra ingestione oraria e non oraria----fare prima una with con la union e poi la ranking
drop table if exists ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.sw_misure_no_12m_202002_unionall;
create table ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.sw_misure_no_12m_202002_unionall as
select * from ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.sw_misure_no_12m_202002
union all
select * from ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.ingestion_orari;