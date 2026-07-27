CREATE TABLE IF NOT EXISTS eng_test.rcugas_massivo_ca_p_20210701 STORED AS PARQUET
LOCATION '/user/hive/warehouse/eng_test.db/rcugas_massivo_ca_p_20210701'
AS
SELECT mass.n_id_pdr, mass.t_codice_pdr, mass.capacita_trasporto, mass.mese_val_cap_trasp, mass.t_cod_tipo_pdr, mass.t_disalimentabilita,
       mass.bilanciamento, mass.n_id_fornitura, mass.d_data_inizio_for, mass.data_fine_for, mass.n_id_az_udd, mass.piva_udd, mass.n_id_az_cc,
       mass.piva_cc, mass.n_id_cliente, mass.t_partita_iva_cli, mass.t_codice_fiscale_cli, mass.b_cf_straniero, mass.t_referente, mass.t_nome_ref, 	
       mass.t_cognome_ref, mass.t_email_ref, mass.t_telefono_ref, mass.t_residenza, mass.data_val_res, mass.t_toponimopdr, mass.t_nomestrada_pdr, 	
       mass.t_civico_pdr, mass.t_cap_pdr, mass.t_comune_istat_pdr, mass.t_comune_pdr, mass.t_provincia_pdr, mass.t_nazione_pdr, mass.altro_ind_pdr, 	
       mass.t_toponimo_forn, mass.t_nomestrada_forn, mass.t_civico_forn, mass.t_cap_forn, mass.t_comune_istatforn, mass.t_comune_forn,
       mass.t_provincia_forn, mass.t_nazione_forn, mass.altro_ind_forn, mass.t_accesso_ui, mass.t_tipo_fornitura, mass.t_aliquota_iva,
       mass.t_aliquota_accise, mass.t_add_regionale, mass.t_altre_info_imposte, mass.t_matricola_misuratore, mass.t_classe_misuratore,
       mass.t_tipo_misuratore, mass.t_telegestione, mass.t_pre_conv, mass.t_matricola_convertitore, mass.n_num_cifre_convertitore,
       mass.t_anno_fabbric_convertitore, mass.t_data_inst_convertitore, mass.n_coeff_correzione, mass.press_misure, mass.t_access_misuratore,
       mass.n_num_cifre_misuratore, mass.t_anno_fabbric_misuratore, mass.t_data_inst_misuratore, mass.t_misuratore_integrato, mass.n_potenzialita_massima,
       mass.n_potenzialita_tot_installata, mass.n_max_prelievo_orario, mass.t_erog_servizio_energ, mass.t_partita_iva_gestcal, mass.t_ragione_sociale_gestcal,
       mass.t_telefono_gestcal, mass.t_email_gestcal, mass.t_toponimo_gestcal, mass.t_nomestrada_gestcal, mass.t_civico_gestcal, mass.t_cap_gestcal, mass.t_comune_istat_gestcal,
       mass.t_comune_gestcal, mass.t_provincia_gestcal, mass.t_nazione_gestcal, mass.t_indirizzo_completo, mass.d_data_rif_pdr, mass.d_aggiornamento_pdr,
       mass.d_data_rif_tecn, mass.d_aggiornamento_tecn, mass.d_data_rif_mis, mass.d_aggiornamento_mis, mass.d_data_rif_forn, mass.d_aggiornamento_forn, mass.t_tipo_bonus,
       mass.d_data_inizio_erog_bonus, mass.d_data_fine_erog_bonus, mass.d_data_rif_bonus, mass.d_aggiornamento_bonus, mass.d_data_aggiornamento, mass.n_id_udd,
       mass.n_id_venditore, mass.t_cod_profilo, mass.t_cod_cat_uso, mass.t_cod_classe_prelievo, mass.t_anno_termico, mass.d_data_rif_prel, mass.t_trattamento, mass.t_toponimo_esaz,
       mass.t_nomestrada_esaz, mass.t_civico_esaz, mass.t_cap_esaz, mass.t_comune_istat_esaz, mass.t_comune_esaz, mass.t_provincia_esaz, mass.t_nazione_esaz, mass.altro_ind_esaz,
       mass.t_codice_ateco, mass.t_pagamento_iva, mass.t_codice_ufficio, mass.t_cf_intestatario_fatt, mass.t_cf_straniero_fatt, mass.t_piva_intestatario_fatt, mass.t_nome_intestatario_fatt,
       mass.t_cognome_intestatario_fatt, mass.t_rag_soc_intestatario_fatt, mass.t_anno_mese_rinn_bonus, mass.d_data_inizio_bonus, mass.d_data_fine_bonus, mass.n_prelievo_annuo,
       mass.t_fattore_correz_climatica, mass.t_altro_ind_gestcal, mass.t_tipo_op, mass.t_processo, mass.n_id_pratica_processo, conn2_p.id_regione_climatica
FROM (
      SELECT b.n_id_pdr, b.t_codice_pdr, b.capacita_trasporto, b.mese_val_cap_trasp, b.t_cod_tipo_pdr, b.t_disalimentabilita,
             b.bilanciamento, b.n_id_fornitura, b.d_data_inizio_for, 
             CASE WHEN nvl(b.data_fine_for_case,'2999-12-31 00:00:00.0') = b.max_data_fine_for_case 
             AND date_format(nvl(b.data_fine_for_case,'2999-12-31 00:00:00.0'),'yyyy-MM-dd') > from_unixtime(unix_timestamp('2021-07-01','yyyy-MM-dd'),'yyyy-MM-dd')
                  THEN null ELSE b.data_fine_for_case END data_fine_for,
             b.n_id_az_udd, b.piva_udd, b.n_id_az_cc,
             b.piva_cc, b.n_id_cliente, b.t_partita_iva_cli, b.t_codice_fiscale_cli, b.b_cf_straniero, b.t_referente, b.t_nome_ref,
             b.t_cognome_ref, b.t_email_ref, b.t_telefono_ref, b.t_residenza, b.data_val_res, b.t_toponimopdr, b.t_nomestrada_pdr,
             b.t_civico_pdr, b.t_cap_pdr, b.t_comune_istat_pdr, b.t_comune_pdr, b.t_provincia_pdr, b.t_nazione_pdr, b.altro_ind_pdr,
             b.t_toponimo_forn, b.t_nomestrada_forn, b.t_civico_forn, b.t_cap_forn, b.t_comune_istatforn, b.t_comune_forn,
             b.t_provincia_forn, b.t_nazione_forn, b.altro_ind_forn, b.t_accesso_ui, b.t_tipo_fornitura, b.t_aliquota_iva,
             b.t_aliquota_accise, b.t_add_regionale, b.t_altre_info_imposte, b.t_matricola_misuratore, b.t_classe_misuratore,
             b.t_tipo_misuratore, b.t_telegestione, b.t_pre_conv, b.t_matricola_convertitore, b.n_num_cifre_convertitore,
             b.t_anno_fabbric_convertitore, b.t_data_inst_convertitore, b.n_coeff_correzione, b.press_misure, b.t_access_misuratore,
             b.n_num_cifre_misuratore, b.t_anno_fabbric_misuratore, b.t_data_inst_misuratore, b.t_misuratore_integrato, b.n_potenzialita_massima,
             b.n_potenzialita_tot_installata, b.n_max_prelievo_orario, b.t_erog_servizio_energ, b.t_partita_iva_gestcal, b.t_ragione_sociale_gestcal,
             b.t_telefono_gestcal, b.t_email_gestcal, b.t_toponimo_gestcal, b.t_nomestrada_gestcal, b.t_civico_gestcal, b.t_cap_gestcal, b.t_comune_istat_gestcal,
             b.t_comune_gestcal, b.t_provincia_gestcal, b.t_nazione_gestcal, b.t_indirizzo_completo, b.d_data_rif_pdr, b.d_aggiornamento_pdr,
             b.d_data_rif_tecn, b.d_aggiornamento_tecn, b.d_data_rif_mis, b.d_aggiornamento_mis, b.d_data_rif_forn, b.d_aggiornamento_forn, b.t_tipo_bonus,
             b.d_data_inizio_erog_bonus, b.d_data_fine_erog_bonus, b.d_data_rif_bonus, b.d_aggiornamento_bonus, b.d_data_aggiornamento, b.n_id_udd,
             b.n_id_venditore, b.t_cod_profilo, b.t_cod_cat_uso, b.t_cod_classe_prelievo, b.t_anno_termico, b.d_data_rif_prel, b.t_trattamento, b.t_toponimo_esaz,
             b.t_nomestrada_esaz, b.t_civico_esaz, b.t_cap_esaz, b.t_comune_istat_esaz, b.t_comune_esaz, b.t_provincia_esaz, b.t_nazione_esaz, b.altro_ind_esaz,
             b.t_codice_ateco, b.t_pagamento_iva, b.t_codice_ufficio, b.t_cf_intestatario_fatt, b.t_cf_straniero_fatt, b.t_piva_intestatario_fatt, b.t_nome_intestatario_fatt,
             b.t_cognome_intestatario_fatt, b.t_rag_soc_intestatario_fatt, b.t_anno_mese_rinn_bonus, b.d_data_inizio_bonus, b.d_data_fine_bonus, b.n_prelievo_annuo,
             b.t_fattore_correz_climatica, b.t_altro_ind_gestcal, b.t_tipo_op, b.t_processo, b.n_id_pratica_processo
      FROM(
           SELECT a.n_id_pdr, a.t_codice_pdr, a.capacita_trasporto, a.mese_val_cap_trasp, a.t_cod_tipo_pdr, a.t_disalimentabilita,
                  a.bilanciamento, a.n_id_fornitura, a.d_data_inizio_for, a.data_fine_for AS data_fine_for_case, a.n_id_az_udd, a.piva_udd, a.n_id_az_cc,
                  a.piva_cc, a.n_id_cliente, a.t_partita_iva_cli, a.t_codice_fiscale_cli, a.b_cf_straniero, a.t_referente, a.t_nome_ref,
                  a.t_cognome_ref, a.t_email_ref, a.t_telefono_ref, a.t_residenza, a.data_val_res, a.t_toponimopdr, a.t_nomestrada_pdr,
                  a.t_civico_pdr, a.t_cap_pdr, a.t_comune_istat_pdr, a.t_comune_pdr, a.t_provincia_pdr, a.t_nazione_pdr, a.altro_ind_pdr,
                  a.t_toponimo_forn, a.t_nomestrada_forn, a.t_civico_forn, a.t_cap_forn, a.t_comune_istatforn, a.t_comune_forn,
                  a.t_provincia_forn, a.t_nazione_forn, a.altro_ind_forn, a.t_accesso_ui, a.t_tipo_fornitura, a.t_aliquota_iva,
                  a.t_aliquota_accise, a.t_add_regionale, a.t_altre_info_imposte, a.t_matricola_misuratore, a.t_classe_misuratore,
                  a.t_tipo_misuratore, a.t_telegestione, a.t_pre_conv, a.t_matricola_convertitore, a.n_num_cifre_convertitore,
                  a.t_anno_fabbric_convertitore, a.t_data_inst_convertitore, a.n_coeff_correzione, a.press_misure, a.t_access_misuratore,
                  a.n_num_cifre_misuratore, a.t_anno_fabbric_misuratore, a.t_data_inst_misuratore, a.t_misuratore_integrato, a.n_potenzialita_massima,
                  a.n_potenzialita_tot_installata, a.n_max_prelievo_orario, a.t_erog_servizio_energ, a.t_partita_iva_gestcal, a.t_ragione_sociale_gestcal,
                  a.t_telefono_gestcal, a.t_email_gestcal, a.t_toponimo_gestcal, a.t_nomestrada_gestcal, a.t_civico_gestcal, a.t_cap_gestcal, a.t_comune_istat_gestcal,
                  a.t_comune_gestcal, a.t_provincia_gestcal, a.t_nazione_gestcal, a.t_indirizzo_completo, a.d_data_rif_pdr, a.d_aggiornamento_pdr,
                  a.d_data_rif_tecn, a.d_aggiornamento_tecn, a.d_data_rif_mis, a.d_aggiornamento_mis, a.d_data_rif_forn, a.d_aggiornamento_forn, a.t_tipo_bonus,
                  a.d_data_inizio_erog_bonus, a.d_data_fine_erog_bonus, a.d_data_rif_bonus, a.d_aggiornamento_bonus, a.d_data_aggiornamento, a.n_id_udd,
                  a.n_id_venditore, a.t_cod_profilo, a.t_cod_cat_uso, a.t_cod_classe_prelievo, a.t_anno_termico, a.d_data_rif_prel, a.t_trattamento, a.t_toponimo_esaz,
                  a.t_nomestrada_esaz, a.t_civico_esaz, a.t_cap_esaz, a.t_comune_istat_esaz, a.t_comune_esaz, a.t_provincia_esaz, a.t_nazione_esaz, a.altro_ind_esaz,
                  a.t_codice_ateco, a.t_pagamento_iva, a.t_codice_ufficio, a.t_cf_intestatario_fatt, a.t_cf_straniero_fatt, a.t_piva_intestatario_fatt, a.t_nome_intestatario_fatt,
                  a.t_cognome_intestatario_fatt, a.t_rag_soc_intestatario_fatt, a.t_anno_mese_rinn_bonus, a.d_data_inizio_bonus, a.d_data_fine_bonus, a.n_prelievo_annuo,
                  a.t_fattore_correz_climatica, a.t_altro_ind_gestcal, a.t_tipo_op, a.t_processo, a.n_id_pratica_processo,
           	    max(nvl(a.data_fine_for,'2999-12-31 00:00:00.0')) over (partition by a.t_codice_pdr) AS max_data_fine_for_case
           FROM rcugas.rcugas_massivo_p AS a
           WHERE nvl(a.n_id_fornitura,'') != ''
           AND date_format(nvl(a.d_data_inizio_for,'1492-12-31 00:00:00.0'),'yyyy-MM-dd')
               <= from_unixtime(unix_timestamp('2021-07-01','yyyy-MM-dd'),'yyyy-MM-dd')
      	 AND a.n_id_pdr IN (SELECT cc.n_id_pdr
      	                    FROM rcugas.rcugas_massivo_p AS cc
      						WHERE from_unixtime(unix_timestamp('2021-07-01','yyyy-MM-dd'),'yyyy-MM-dd')
      						BETWEEN date_format(nvl(cc.d_data_inizio_for,'1492-12-31 00:00:00.0'),'yyyy-MM-dd')
      					    AND date_format(nvl(cc.data_fine_for,'2999-12-31 00:00:00.0'),'yyyy-MM-dd')
      						GROUP BY cc.n_id_pdr
      						)
      ) AS b 
) AS mass
LEFT OUTER JOIN (
                 SELECT conn2.n_id_pdr,
                        conn2.id_regione_climatica
                 FROM rcugas.rcugas_connessioni_distr2_p as conn2
                 WHERE from_unixtime(unix_timestamp('2021-07-01','yyyy-MM-dd'),'yyyy-MM-dd')
                 BETWEEN date_format(nvl(conn2.d_data_inizio_conn,'1492-12-31 00:00:00.0'),'yyyy-MM-dd')
                 AND date_format(nvl(conn2.d_data_fine_conn,'2999-12-31 00:00:00.0'),'yyyy-MM-dd')
                 GROUP BY conn2.n_id_pdr, 
				          conn2.id_regione_climatica) as conn2_p
ON mass.n_id_pdr = conn2_p.n_id_pdr
;



CREATE TABLE IF NOT EXISTS rcugas.rcugas_tech_ca_p_20210701 STORED AS PARQUET
LOCATION '/user/hive/warehouse/rcugas.db/rcugas_tech_ca_p_20210701'
AS
SELECT tt.t_codice_pdr,
       tt.n_id_pdr,
	   bb.t_matricola_misuratore,
	   bb.t_matricola_convertitore,
	   bb.t_misuratore_integrato,
	   bb.t_pre_conv,
	   bb.n_coeff_correzione,
	   bb.n_num_cifre_misuratore,
	   bb.n_num_cifre_convertitore,
	   bb.data_inizio_tech,
	   bb.data_fine_tech
FROM (SELECT a.t_codice_pdr, a.n_id_pdr
      FROM rcugas.rcugas_massivo_ca_p_20210701 as a
	  GROUP BY t_codice_pdr, n_id_pdr ) as tt,
      (SELECT mis.n_id_pdr,
              mis.t_matricola_misuratore,
              conve.t_matricola_convertitore,
              mis.t_misuratore_integrato as t_misuratore_integrato,
              mis.t_presenza_convertitore as t_pre_conv,
              mis.n_coeff_correzione,
			  mis.n_num_cifre_misuratore,
			  conve.n_num_cifre_convertitore,
              date_format(mis.d_data_inizio,'yyyy-MM-dd') as data_inizio_tech,
              date_format(mis.d_data_fine,'yyyy-MM-dd') as data_fine_tech
       FROM rcugas.rcugas_var_misuratore_p as mis
       LEFT OUTER JOIN (SELECT pp.n_id_pdr,
                               pp.n_id_var_misuratore,
                               pp.t_matricola_convertitore,
                               pp.n_num_cifre_convertitore
                        FROM rcugas.rcugas_var_convertitore_p as pp) as conve
       ON mis.n_id_pdr = conve.n_id_pdr) as bb
WHERE tt.n_id_pdr = bb.n_id_pdr
;