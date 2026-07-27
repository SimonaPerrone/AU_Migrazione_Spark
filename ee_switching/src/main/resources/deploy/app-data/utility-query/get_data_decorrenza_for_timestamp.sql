with data_info as
(
select
max(d_data_decorrenza) as max_d_data_decorrenza,
min(d_data_decorrenza) as min_d_data_decorrenza,
date_format(to_date(max(anno_mese_calc)),'yyyy-MM-dd') as first_date
from ${hiveconf:SWITCHING_EE_HIVE_DB_NAME}.${hiveconf:SWITCHING_EE_HIVE_STORICI_RS_TRATT_TABLE_NAME}
where d_creazione = '${hiveconf:timestamp_par}'
)
select
case when max_d_data_decorrenza = min_d_data_decorrenza
then concat('>>>DATA-SINGOLA:',first_date,' ',max_d_data_decorrenza,'<<<')
else
concat('>>>DATA-DOPPIA:',first_date,' ',date_format(to_date(add_months(to_date(first_date),+1)),'yyyy-MM-dd'),'<<<')
end
from data_info



