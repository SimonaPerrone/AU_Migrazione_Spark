---TODO: aggiungere un ulteriore ranking per i flussi doppi tra ingestione oraria e non oraria----fare prima una with con la union e poi la ranking
drop table sos_202011_202012.sw_misure_no_12m_202002_unionall;
create table sos_202011_202012.sw_misure_no_12m_202002_unionall as
select * from sos_202011_202012.sw_misure_no_12m_202002
union all
select * from sos_202011_202012.ingestion_orari;