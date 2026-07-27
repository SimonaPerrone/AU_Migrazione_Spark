DROP TABLE misuregas.RCUGAS_CONNESSIONI_DISTR;
CREATE TABLE misuregas.RCUGAS_CONNESSIONI_DISTR stored as parquet as       
select A.* from RCUGAS.RCUGAS_CONNESSIONI_DISTR_p A
inner join
 (select max(d_data_inizio_conn)max_d_init,n_id_pdr from
 RCUGAS.RCUGAS_CONNESSIONI_DISTR_p
 group by n_id_pdr
 ) max_distr on A.n_id_pdr = max_distr.n_id_pdr AND nvl(max_distr.max_d_init,'')= nvl(A.d_data_inizio_conn,''); 

DROP TABLE misuregas.v_RCUGAS_DISTRIBUTORE;
CREATE TABLE misuregas.v_RCUGAS_DISTRIBUTORE stored as parquet as   
select v_RCUGAS_DISTRIBUTORE.t_rag_soc,v_RCUGAS_DISTRIBUTORE.n_id_distributore
from RCUGAS.v_RCUGAS_DISTRIBUTORE_p as v_RCUGAS_DISTRIBUTORE
where v_RCUGAS_DISTRIBUTORE.t_rag_soc  is not null and  v_RCUGAS_DISTRIBUTORE.t_rag_soc <> ''
and NVL(D_DATA_FINE,'') = '';

