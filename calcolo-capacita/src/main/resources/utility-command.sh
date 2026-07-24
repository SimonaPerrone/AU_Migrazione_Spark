execute_sqoop_import()
{
    local schema="$1"
    local table_in="$2"
    local split_column="$3"
    local table_out="$4"
    sqoop import \
        --connect "${JDBC_URL}" \
        --username "${JDBC_USERNAME}" \
        --password "${JDBC_PASSWORD}" \
        --table "${schema}.${table_in}" \
        --hive-import \
        --hive-database "eng_test" \
        --hive-table "${table_out}" \
        --create-hive-table \
        --split-by "${split_column}" \
        --num-mappers 5 \
        --validate
}
execute_sqoop_import()
{
    local schema="$1"
    local table_in="$2"
    local split_column="$3"
    local table_out="$4"
    sqoop import \
        --connect "${JDBC_URL}" \
        --username "${JDBC_USERNAME}" \
        --password "${JDBC_PASSWORD}" \
        --table "${schema}.${table_in}" \
        --hive-import \
        --hive-database "eng_test" \
        --hive-table "${table_out}" \
        --create-hive-table \
        -n 5
}
execute_sqoop_export(){
  local export_dir_path="$1"
  local table_out="$2"
  sqoop export \
    --connect "${JDBC_URL}" \
    --username "${JDBC_USERNAME}" \
    --password "${JDBC_PASSWORD}" \
    --table "${table_out}" \
    --staging-table "${table_out}_STAGING" \
    --clear-staging-table \
    --export-dir ${export_dir_path} \
    --input-fields-terminated-by ',' \
    --input-lines-terminated-by '\n' \
    --input-null-string '\\N' \
    --input-null-non-string '\\N' \
    --num-mappers 1
}
load_jdbc(){
  # load bash JDBC variables from Java properties file in HDFS
source <(hdfs dfs -cat '/user/eng_test/calcolo_capacita/oracle_collaudo.properties' \
    | sed 's/\(.+\?\)\=\(.*\)$/\1='\''\2'\''/g' \
    | grep -P 'spark\.app\.user|spark\.app\.url|spark\.app\.password' \
    | sed 's/spark\.app\.user/JDBC_USERNAME/g' \
    | sed 's/spark\.app\.url/JDBC_URL/g' \
    | sed 's/spark\.app\.password/JDBC_PASSWORD/g' \
    )
}
execute_sqoop_query()
{
    local query="$1"
    sqoop eval --verbose --connect "${JDBC_URL}" --username "${JDBC_USERNAME}" --password "${JDBC_PASSWORD}" --query "${query}"
}
load_jdbc
execute_sqoop_query "SELECT COUNT(*) FROM CLG.clg_perimetro_pdr_puntuale_v "

execute_sqoop_query "SELECT COUNT(*) FROM CLG.CLG_PERIMETRO_REMI_V"
execute_sqoop_query "select text from ALL_VIEWS where upper(view_name) like upper(CLG_PERIMETRO_REMI_V)"
execute_sqoop_query "select text from ALL_VIEWS where upper(view_name) like upper('CLG_PERIMETRO_REMI_V')"

------------------------
| COUNT(*)             |
------------------------
| 4116                 |
------------------------

execute_sqoop_query "SELECT count(distinct(n_id_remi)),COUNT(*) FROM CLG.CLG_PERIMETRO_REMI_V"
-----------------------------------------------
| COUNT(DISTINCT(N_ID_REMI)) | COUNT(*)             |
-----------------------------------------------
| 4116                 | 4116                 |
-----------------------------------------------

execute_sqoop_query "SELECT COUNT(*) FROM CLG.CLG_PERIMETRO_PDR_V"
execute_sqoop_query "SELECT COUNT(*) FROM TMP_CLG_PERIMETRO_remi"

beeline -u 'jdbc:hive2://dmphclo07:10000' -n 'eng_test' -e "select count(distinct(n_id_pdr)),count(*) from eng_test.CLG_PERIMETRO_PDR_V"
execute_sqoop_query "SELECT DBMS_METADATA.GET_DDL('TABLE','CLG.CLG_PERIMETRO_REMI_V') FROM USER_TABLES u"

execute_sqoop_query "SELECT COUNT(*) FROM CLG.CLG_PERIMETRO_PDR_V"

------------------------
| COUNT(*)             |
------------------------
| 30356                |
------------------------

execute_sqoop_query "SELECT count(distinct(n_id_remi)),COUNT(*) FROM CLG.CLG_PERIMETRO_PDR_V"
-----------------------------------------------
| COUNT(DISTINCT(N_ID_REMI)) | COUNT(*)             |
-----------------------------------------------
| 2363                 | 30356                |

execute_sqoop_query "SELECT count(distinct(n_id_pdr)),COUNT(*) FROM CLG.CLG_PERIMETRO_PDR_V"
execute_sqoop_import "CLG" "CLG_PERIMETRO_PDR_V" "n_id_pdr" "CLG_PERIMETRO_PDR_V"
create_remi="CREATE TABLE TMP_CLG_PERIMETRO_REMI_2 AS (SELECT to_char(n_id_remi) n_id_remi,remi_pool,to_char(n_id_remi_anagrafica) n_id_remi_anagrafica, data_calc ,t_tariffa ,t_z, t_pmax FROM MADDARII.TMP_CLG_PERIMETRO_REMI)"
create_pdr="CREATE TABLE TMP_CLG_PERIMETRO_PDR_2 AS (SELECT data_calc, to_char(n_id_pdr)	n_id_pdr, to_char(t_codice_pdr) t_codice_pdr, n_prelievo_annuo, to_char(n_id_remi) n_id_remi FROM MADDARII.TMP_CLG_PERIMETRO_PDR)"

execute_sqoop_query "select count(*) from TMP_CLG_PERIMETRO_REMI"
------------------------
| COUNT(*)             |
------------------------
| 4060                 |
------------------------

execute_sqoop_query "${create_remi}"
execute_sqoop_query "select count(*) from TMP_CLG_PERIMETRO_REMI_2"
------------------------
| COUNT(*)             |
------------------------
| 4060                 |
------------------------

execute_sqoop_query "select count(*) from TMP_CLG_PERIMETRO_PDR"
------------------------
| COUNT(*)             |
------------------------
| 27378                |
------------------------

execute_sqoop_query "${create_pdr}"
execute_sqoop_query "select count(*) from TMP_CLG_PERIMETRO_PDR_2"
------------------------
| COUNT(*)             |
------------------------
| 27378                |
------------------------


execute_sqoop_import "CLG" "CLG_PERIMETRO_REMI_V" "CLG_PERIMETRO_REMI_V"
execute_sqoop_import "CLG" "CLG_PERIMETRO_PDR_V" "CLG_PERIMETRO_PDR_V"

insert overwrite table eng_test.clg_anagrafica_gm_view
select
 t1.n_id_pdr,
 t1.data_calc,
 t1.n_id_remi,
 t1.t_codice_pdr,
 t1.n_prelievo_annuo,
 t2.t_tariffa,
 t2.t_z,
 t2.t_pmax
from  eng_test.TMP_CLG_PERIMETRO_PDR_2_TMP t1
inner join eng_test.TMP_CLG_PERIMETRO_REMI_2_TMP t2
on  t1.n_id_remi=t2.n_id_remi;


select count(distinct t_codice_pdr),count(*) from clg_anagrafica_gm_view;
27375	27375

INSERT INTO TABLE eng_test.clg_anagrafica_gm_view VALUES ('100000', '100000','100000' , '99999999999999',12,13,14,11)

RUN SPARK OK:
2021/02/15
y=60
x=40

cheange type

execute_sqoop_query  "DELETE FROM CLG_PDR_CAPACITA_TMP";
execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP_STAGING MODIFY n_pcm NUMBER(11,2)";
execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP_STAGING MODIFY n_cl NUMBER(11,2)";
execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP_STAGING MODIFY n_ctc NUMBER(11,2)";

create table result_export_test like eng_test.clg_pdr_capacita_tmp;
insert overwrite table result_export_test
select * from clg_pdr_capacita_tmp
limit 10;
execute_sqoop_export "/user/hive/warehouse/eng_test.db/result_export_test" "CLG_PDR_CAPACITA_TMP"

execute_sqoop_query "SELECT DBMS_METADATA.GET_DDL('TABLE','CLG_PDR_CAPACITA_TMP') FROM USER_TABLES u"
execute_sqoop_query "SELECT table_name, owner FROM all_tables WHERE owner='CLG' ORDER BY owner, table_name"

execute_sqoop_query "select count(*) from CLG_PDR_CAPACITA_TMP"
execute_sqoop_query  "DELETE FROM CLG_PDR_CAPACITA_TMP"
#36.800000000000004 no problem
#36.8000000000000040000
execute_sqoop_export "/user/hive/warehouse/eng_test.db/result_export_test" "CLG_PDR_CAPACITA_TMP"
execute_sqoop_query "select * from CLG_PDR_CAPACITA_TMP"

drop table eng_test.result_export_test;
CREATE TABLE IF NOT EXISTS eng_test.result_export_test(
id_pdr_capacita_tmp string, --valorizzato da altri applicativi
n_id_pdr string,
n_id_pratica string, --valorizzato da altri applicativi
t_codice_pdr string,
d_data_calcolo string,
t_tipo_calcolo string,
n_pcm decimal (8,2),
n_ctc decimal (8,2),
n_cl decimal (8,2),
d_data_inizio string,
d_data_fine string,
t_origine string,
t_processo string,
d_data_inserimento string,
t_esito_agg_rcu string, --valorizzato da altri applicativi
t_errore_agg_rcu string, --valorizzato da altri applicativi
t_esito_agg_rcu_desc string, --valorizzato da altri applicativi
t_stato  string, --valorizzato da altri applicativi
d_data_aggiornamento  string, --valorizzato da altri applicativi
n_execution_id string --valorizzato da altri applicativi
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

insert overwrite table result_export_test
select * from clg_pdr_capacita_tmp;

execute_sqoop_query  "DELETE FROM CLG_PDR_CAPACITA_TMP"
execute_sqoop_export "/user/hive/warehouse/eng_test.db/result_export_test" "CLG_PDR_CAPACITA_TMP"

#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_pcm BINARY_DOUBLE";
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_cl BINARY_DOUBLE";
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_ctc BINARY_DOUBLE";
#
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_pcm NUMBER(38,127)";
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_cl NUMBER(38,127)";
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_ctc NUMBER(38,127)";
#
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_pcm NUMBER(38,127)";
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_cl NUMBER(38,127)";
#execute_sqoop_query  "ALTER TABLE CLG_PDR_CAPACITA_TMP MODIFY n_ctc NUMBER(38,127)";

execute_sqoop_query  "CREATE TABLE CLG.CLG_PDR_CAPACITA_TMP_STAGING AS (SELECT * FROM CLG.CLG_PDR_CAPACITA_TMP)"