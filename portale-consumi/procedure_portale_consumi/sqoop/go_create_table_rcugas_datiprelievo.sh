#!/bin/bash

F_LOG=$1

tabella_datiprelievo=rcugas.rcugas_pdr_datiprelievo

echo "$(date) Creazione tabella ${tabella_datiprelievo} in corso..."
echo "$(date) Creazione tabella ${tabella_datiprelievo} in corso..." &>> "$F_LOG"


rm -r drop_tab_supporto_RCUGAS_PDR_DATIPRELIEVO.hql
rm -f esito_hive_${tabella_datiprelievo}.log

# Costruzione RCUGAS.RCUGAS_PDR_DATIPRELIEVO_P
tabelle_supporto_schemi=( \
         rcugas.v_rcugas_var_prel_annuo "SELECT
          pa.D_AGGIORNAMENTO PA_D_AGGIORNAMENTO,
          pa.N_ID_VAR_PREL_ANNUO PA_N_ID_VAR_PREL_ANNUO,
          pa.N_ID_PDR,
          pa.T_ANNO,
          pa.N_PRELIEVO_ANNUO,
          pa.T_NOTE PA_T_NOTE,
          pa.N_ID_TRACCIA PA_N_ID_TRACCIA,
          pa.N_ID_S_PREC PA_N_ID_S_PREC,
          pa.D_DATA_RIF PA_D_DATA_RIF
          FROM (SELECT MAX (D_DATA_INIZIO) data_inizio_max,n_id_pdr,t_anno FROM RCUGAS.RCUGAS_VAR_PREL_ANNUO_P GROUP BY n_ID_PDR,T_ANNO) x
          JOIN RCUGAS.RCUGAS_VAR_PREL_ANNUO_P PA on x.data_inizio_max=pa.d_data_inizio and x.n_id_pdr=pa.n_id_pdr and x.T_ANNO=pa.T_ANNO and x.t_anno=pa.t_anno" \
		 rcugas.v_rcugas_var_trattamento "SELECT
		  TR.D_AGGIORNAMENTO TR_D_AGGIORNAMENTO,
		  TR.N_ID_VAR_TRATTAMENTO TR_N_ID_VAR_TRATTAMENTO,
          TR.N_ID_PDR,
          TR.T_ANNO,
          TR.T_NOTE TR_T_NOTE,
          TR.N_ID_TRACCIA TR_N_ID_TRACCIA,
          TR.N_ID_S_PREC TR_N_ID_S_PREC,
		  TR.D_DATA_RIF TR_D_DATA_RIF,
          TR.T_TRATTAMENTO_SETTLEMENT	
          FROM (SELECT MAX (D_DATA_INIZIO) data_inizio_max,n_id_pdr,t_anno FROM RCUGAS.RCUGAS_VAR_TRATTAMENTO_P GROUP BY n_ID_PDR,T_ANNO) x
          JOIN RCUGAS.RCUGAS_VAR_TRATTAMENTO_P tr on x.data_inizio_max=tr.d_data_inizio and x.n_id_pdr=tr.n_id_pdr and x.T_ANNO=tr.T_ANNO and x.t_anno=tr.t_anno" \
		 rcugas.v_rcugas_var_profilo "select 
		  PR.D_AGGIORNAMENTO PR_D_AGGIORNAMENTO,
		  PR.N_ID_VAR_PROFILO PR_N_ID_VAR_PROFILO,
          PR.N_ID_PDR,
          PR.T_ANNO,
          PR.T_COD_PROFILO,
          PR.T_COD_CAT_USO,
          PR.T_COD_CLASSE_PRELIEVO,
          PR.T_NOTE PR_T_NOTE,
          PR.N_ID_TRACCIA PR_N_ID_TRACCIA,
		  PR.N_ID_S_PREC PR_N_ID_S_PREC,
		  PR.D_DATA_RIF PR_D_DATA_RIF 
          FROM (SELECT MAX (D_DATA_INIZIO) data_inizio_max,n_id_pdr,t_anno FROM RCUGAS.RCUGAS_VAR_PROFILO_P GROUP BY n_ID_PDR,T_ANNO) x
          JOIN RCUGAS.RCUGAS_VAR_PROFILO_P pr on x.data_inizio_max=pr.d_data_inizio and x.n_id_pdr=pr.n_id_pdr and x.T_ANNO=pr.T_ANNO and x.t_anno=pr.t_anno" \
)

for ((i = 0; i < ${#tabelle_supporto_schemi[@]}; i++))
do
        if [ $((i%2)) -eq 0 ]
        then
                tabella=${tabelle_supporto_schemi[$i]}
                query=${tabelle_supporto_schemi[$i+1]}

                echo ""
                echo "$(date) Creazione tabella ${tabella^^}_P in corso..."

                echo "$(date) Creazione tabella ${tabella^^}_P in corso..." &>> "$F_LOG" \
                && ( \
                        rm -f ${tabella}.hql \
                        && echo "DROP TABLE IF EXISTS ${tabella}_p;" >> ${tabella}.hql \
                        && echo "CREATE TABLE ${tabella}_p STORED AS PARQUET LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/${tabella}_p' AS ${query};" >> ${tabella}.hql \
                ) && ( \
                        hive -f ${tabella}.hql \
                        && rm -f ${tabella}.hql \
                ) &> esito_hive_${tabella}.log \
                && ( \
                        ([ $(cat esito_hive_${tabella}.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Creazione tabella  ${tabella^^}_p completata con successo." > esito_hive_${tabella}.log ) \
                        || echo "$(date) ERRORE nella creazione della tabella ${tabella^^}_p " &>> esito_hive_${tabella}.log \
                )

                cat esito_hive_${tabella}.log &>> "$F_LOG"
				
				echo "DROP TABLE IF EXISTS ${tabella}_p;" >> drop_tab_supporto_RCUGAS_PDR_DATIPRELIEVO.hql

                rm -f esito_hive_${tabella}.log

        fi
done

query_datiprelievo="
SELECT
           CASE
               WHEN PA_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PA_N_ID_VAR_PREL_ANNUO
               WHEN PR_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PR_N_ID_VAR_PROFILO
               ELSE
                   TR_N_ID_VAR_TRATTAMENTO
           END               AS N_ID_PDR_DATIPRELIEVO,
           N_ID_PDR,
           T_ANNO,
           T_COD_PROFILO,
           N_PRELIEVO_ANNUO,
           CAST(NULL as string)  AS N_LETTURA_CONVERTITORE,
           T_COD_CAT_USO,
           T_COD_CLASSE_PRELIEVO,
           CASE
               WHEN PA_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PA_T_NOTE
               WHEN PR_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PR_T_NOTE
               ELSE
                   TR_T_NOTE
           END               AS T_NOTE,
           CASE
               WHEN PA_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PA_D_AGGIORNAMENTO
               WHEN PR_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PR_D_AGGIORNAMENTO
               ELSE
                   TR_D_AGGIORNAMENTO
           END               AS D_AGGIORNAMENTO,
           CASE
               WHEN PA_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PA_N_ID_TRACCIA
               WHEN PR_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PR_N_ID_TRACCIA
               ELSE
                   TR_N_ID_TRACCIA
           END               AS N_ID_TRACCIA,
           CASE
               WHEN PA_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PA_N_ID_S_PREC
               WHEN PR_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PR_N_ID_S_PREC
               ELSE
                   TR_N_ID_S_PREC
           END               AS N_ID_S_PREC,
           CASE
               WHEN PA_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PA_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PA_D_DATA_RIF
               WHEN PR_D_AGGIORNAMENTO >=
                         NVL (PA_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (PR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
					AND PR_D_AGGIORNAMENTO >=
                         NVL (TR_D_AGGIORNAMENTO,
                              from_unixtime(unix_timestamp('01-01-1900' , 'dd-MM-yyyy')))
               THEN
                   PR_D_DATA_RIF
               ELSE
                   TR_D_DATA_RIF
           END               AS D_DATA_RIF,
           concat(T_ANNO,'01')    T_ANNO_MESE_RIF,
           cast(NULL as string)   AS T_FATTORE_CORREZ_CLIMATICA,
           T_TRATTAMENTO_SETTLEMENT
      FROM (  SELECT MAX (PA_D_AGGIORNAMENTO)
                         PA_D_AGGIORNAMENTO,
                     MAX (PR_D_AGGIORNAMENTO)
                         PR_D_AGGIORNAMENTO,
                     MAX (TR_D_AGGIORNAMENTO)
                         TR_D_AGGIORNAMENTO,
                     MAX (PA_N_ID_VAR_PREL_ANNUO)
                         PA_N_ID_VAR_PREL_ANNUO,
                     MAX (PR_N_ID_VAR_PROFILO)
                         PR_N_ID_VAR_PROFILO,
                     MAX (TR_N_ID_VAR_TRATTAMENTO)
                         TR_N_ID_VAR_TRATTAMENTO,
                     N_ID_PDR,
                     T_ANNO,
                     MAX (T_COD_PROFILO)
                         T_COD_PROFILO,
                     MAX (N_PRELIEVO_ANNUO)
                         N_PRELIEVO_ANNUO,
                     MAX (N_LETTURA_CONVERTITORE)
                         N_LETTURA_CONVERTITORE,
                     MAX (T_COD_CAT_USO)
                         T_COD_CAT_USO,
                     MAX (T_COD_CLASSE_PRELIEVO)
                         T_COD_CLASSE_PRELIEVO,
                     MAX (PA_T_NOTE)
                         PA_T_NOTE,
                     MAX (PR_T_NOTE)
                         PR_T_NOTE,
                     MAX (TR_T_NOTE)
                         TR_T_NOTE,
                     MAX (PA_N_ID_TRACCIA)
                         PA_N_ID_TRACCIA,
                     MAX (PR_N_ID_TRACCIA)
                         PR_N_ID_TRACCIA,
                     MAX (TR_N_ID_TRACCIA)
                         TR_N_ID_TRACCIA,
                     MAX (PA_N_ID_S_PREC)
                         PA_N_ID_S_PREC,
                     MAX (PR_N_ID_S_PREC)
                         PR_N_ID_S_PREC,
                     MAX (TR_N_ID_S_PREC)
                         TR_N_ID_S_PREC,
                     MAX (PA_D_DATA_RIF)
                         PA_D_DATA_RIF,
                     MAX (PR_D_DATA_RIF)
                         PR_D_DATA_RIF,
                     MAX (TR_D_DATA_RIF)
                         TR_D_DATA_RIF,
                     --ANNI.T_ANNO,
                     MAX (T_FATTORE_CORREZ_CLIMATICA)
                         T_FATTORE_CORREZ_CLIMATICA,
                     MAX (T_TRATTAMENTO_SETTLEMENT)
                         T_TRATTAMENTO_SETTLEMENT
                FROM (SELECT                          --/*+ USE_NL(PDR PA ) */
                             PA_D_AGGIORNAMENTO,
                             NULL     PR_D_AGGIORNAMENTO,
                             NULL     TR_D_AGGIORNAMENTO,
                             PA_N_ID_VAR_PREL_ANNUO,
                             NULL     PR_N_ID_VAR_PROFILO,
                             NULL     TR_N_ID_VAR_TRATTAMENTO,
                             N_ID_PDR,
                             T_ANNO,
                             NULL     T_COD_PROFILO,
                             N_PRELIEVO_ANNUO,
                             NULL     AS N_LETTURA_CONVERTITORE,
                             NULL     T_COD_CAT_USO,
                             NULL     T_COD_CLASSE_PRELIEVO,
                             PA_T_NOTE,
                             NULL     PR_T_NOTE,
                             NULL     TR_T_NOTE,
                             PA_N_ID_TRACCIA,
                             NULL     PR_N_ID_TRACCIA,
                             NULL     TR_N_ID_TRACCIA,
                             PA_N_ID_S_PREC,
                             NULL     PR_N_ID_S_PREC,
                             NULL     TR_N_ID_S_PREC,
                             PA_D_DATA_RIF,
                             NULL     PR_D_DATA_RIF,
                             NULL     TR_D_DATA_RIF,
                             --ANNI.T_ANNO,
                             NULL     AS T_FATTORE_CORREZ_CLIMATICA,
                             NULL     T_TRATTAMENTO_SETTLEMENT
                        FROM RCUGAS.v_RCUGAS_VAR_PREL_ANNUO_P PA
                      UNION ALL
                      SELECT                           --/*+ USE_NL(PDR PR) */
                             NULL     PA_D_AGGIORNAMENTO,
                             PR_D_AGGIORNAMENTO,
                             NULL     TR_D_AGGIORNAMENTO,
                             NULL     PA_N_ID_VAR_PREL_ANNUO,
                             PR_N_ID_VAR_PROFILO,
                             NULL     TR_N_ID_VAR_TRATTAMENTO,
                             N_ID_PDR,
                             T_ANNO,
                             T_COD_PROFILO,
                             NULL     N_PRELIEVO_ANNUO,
                             NULL     AS N_LETTURA_CONVERTITORE,
                             T_COD_CAT_USO,
                             T_COD_CLASSE_PRELIEVO,
                             NULL     PA_T_NOTE,
                             PR_T_NOTE,
                             NULL     TR_T_NOTE,
                             NULL     PA_N_ID_TRACCIA,
                             PR_N_ID_TRACCIA,
                             NULL     TR_N_ID_TRACCIA,
                             NULL     PA_N_ID_S_PREC,
                             PR_N_ID_S_PREC,
                             NULL     TR_N_ID_S_PREC,
                             NULL     PA_D_DATA_RIF,
                             PR_D_DATA_RIF,
                             NULL     TR_D_DATA_RIF,
                             --ANNI.T_ANNO,
                             NULL     AS T_FATTORE_CORREZ_CLIMATICA,
                             NULL     T_TRATTAMENTO_SETTLEMENT
                        FROM RCUGAS.V_RCUGAS_VAR_PROFILO_P PR
                      UNION ALL
                      SELECT                           --/*+ USE_NL(PDR TR) */
                             NULL     PA_D_AGGIORNAMENTO,
                             NULL     PR_D_AGGIORNAMENTO,
                             TR_D_AGGIORNAMENTO,
                             NULL     PA_N_ID_VAR_PREL_ANNUO,
                             NULL     PR_N_ID_VAR_PROFILO,
                             TR_N_ID_VAR_TRATTAMENTO,
                             N_ID_PDR,
                             T_ANNO,
                             NULL     T_COD_PROFILO,
                             NULL     N_PRELIEVO_ANNUO,
                             NULL     AS N_LETTURA_CONVERTITORE,
                             NULL     T_COD_CAT_USO,
                             NULL     T_COD_CLASSE_PRELIEVO,
                             NULL     PA_T_NOTE,
                             NULL     PR_T_NOTE,
                             TR_T_NOTE,
                             NULL     PA_N_ID_TRACCIA,
                             NULL     PR_N_ID_TRACCIA,
                             TR_N_ID_TRACCIA,
                             NULL     PA_N_ID_S_PREC,
                             NULL     PR_N_ID_S_PREC,
                             TR_N_ID_S_PREC,
                             NULL     PA_D_DATA_RIF,
                             NULL     PR_D_DATA_RIF,
                             TR_D_DATA_RIF,
                             --ANNI.T_ANNO,
                             NULL     AS T_FATTORE_CORREZ_CLIMATICA,
                             T_TRATTAMENTO_SETTLEMENT
                        FROM RCUGAS.v_RCUGAS_VAR_TRATTAMENTO_P TR) xy
            GROUP BY n_id_pdr, t_anno) yz"

echo "$(date) Costruzione tabella ${tabella_datiprelievo} in corso..."
echo "$(date) Costruzione tabella ${tabella_datiprelievo} in corso..." &>> "$F_LOG"

rm -f ${tabella_datiprelievo}.hql \
&& echo "DROP TABLE IF EXISTS ${tabella_datiprelievo}_p;" >> ${tabella_datiprelievo}.hql \
&& echo "CREATE TABLE ${tabella_datiprelievo}_p STORED AS PARQUET LOCATION '/user/hive/warehouse/acquirente_unico/sqoop_portcons/${tabella_datiprelievo}_p' AS ${query_datiprelievo};" >> ${tabella_datiprelievo}.hql

hive -f ${tabella_datiprelievo}.hql &> esito_hive_${tabella_datiprelievo}.log
([ $(cat esito_hive_rcugas.rcugas_pdr_datiprelievo.log | grep "100%" | wc -l) -ge 1 ] && echo "$(date) Creazione tabella ${tabella_datiprelievo^^} completata con successo." > esito_hive_${tabella_datiprelievo}.log ) \
|| echo "$(date) ERRORE nella creazione della tabella ${tabella_datiprelievo^^} " &>> esito_hive_${tabella_datiprelievo}.log
rm -f ${tabella_datiprelievo}.hql


# Drop delle tre tabelle di supporto
hive -f drop_tab_supporto_RCUGAS_PDR_DATIPRELIEVO.hql &>> esito_hive_${tabella_datiprelievo}.log

cat esito_hive_${tabella_datiprelievo}.log &>> "$F_LOG"

rm -f drop_tab_supporto_RCUGAS_PDR_DATIPRELIEVO.hql
rm -f esito_hive_${tabella_datiprelievo}.log											

# Fine costruzione RCUGAS.RCUGAS_PDR_DATIPRELIEVO_P 