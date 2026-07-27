create temporary macro isNumber(s string)
cast (s as double) is not null ;

create temporary macro isNumeric(s string)
s not rlike '[^0-9]';

drop table mongodbs.RCU_POD_DISTR;
create TABLE mongodbs.RCU_POD_DISTR stored as parquet as
select 
            RCU_POD_DISTR.n_id_pod,
            RCU_AZIENDA.T_rag_soc 
        from RCU.RCU_POD_DISTR_p RCU_POD_DISTR 
        join (select CAST(n_id_azienda AS STRING)n_id_azienda,t_rag_soc T_rag_soc,t_piva from rcu.rcu_azienda_p where isNumeric(n_id_azienda)=true limit 2000) AS  RCU_AZIENDA
        on RCU_AZIENDA.n_id_azienda=NVL(RCU_POD_DISTR.n_id_distr,'');   