use rcugas;


--CASE WHEN nvl(b_persona_fisica,'Y') ='Y' OR nvl(b_persona_fisica,'1') ='1' THEN
--                      case nvl(t_codice_fiscale,'') when '' then t_partita_iva else t_codice_fiscale end
--                      ELSE case nvl(t_partita_iva,'') when '' then t_codice_fiscale else t_partita_iva end  END t_cf_piva


drop Table misuregas.forniture_gas;
Create Table misuregas.forniture_gas Stored As Parquet
as
SELECT distinct
   rcugas_fornitura.t_nome,
   rcugas_fornitura.t_cognome,
   rcugas_fornitura.t_partita_iva,
   rcugas_fornitura.t_ragione_sociale,
   rcugas_fornitura.t_codice_fiscale,
   rcugas_fornitura.n_id_cliente AS id,
   rcugas_pdr.t_codice_pdr,
   rcugas_indirizzi.t_cap,
   rcugas_pdr_datiprelievo.t_cod_cat_uso AS categoria_duso,
   rcugas_indirizzi.t_civico,
   rcugas_pdr_misuratore.t_classe_misuratore,
   rcugas_fornitura.n_id_fornitura AS codice_fornitura,
   rcugas_pdr_misuratore.n_coeff_correzione AS coefficiente_conversione,
   rcugas_indirizzi.t_comune,
   rcugas_fornitura.d_data_fine AS data_fine_fornitura,
   rcugas_fornitura.d_data_inizio AS data_inizio_fornitura,
   case 
   when rcugas_fornitura.inizio_num < cast(date_format(date_sub(current_date,${env:limit_gg_gas}),'yyyyMMdd') as int)
        then cast(date_format(date_sub(current_date,${env:limit_gg_gas}),'yyyyMMdd') as int)
   else rcugas_fornitura.inizio_num end AS data_inizio_fornitura_num,
   rcugas_fornitura.fine_num as data_fine_fornitura_num,
   rcugas_pdr_misuratore.t_matricola_misuratore AS matricola_misuratore,
   rcugas_indirizzi.t_nazione,
   rcugas_indirizzi.t_nomestrada,
   v_rcu_azienda.t_piva AS piva_cc,
   rcugas_indirizzi.t_provincia,
   v_rcu_azienda.t_rag_soc AS ragione_sociale_cc,
   v_RCUGAS_DISTRIBUTORE.t_rag_soc AS ragione_sociale_distributore,
   rcugas_residenza.t_residenza AS residente,
   rcugas_fornitura.t_tipo_fornitura AS tipo_fornitura,
   rcugas_pdr.t_cod_tipo_pdr AS tipo_pdr,
   rcugas_indirizzi.t_toponimo AS toponimo_indirizzo,
   CAST(NULL AS bigint) AS data_inizio_processo_gdm,
   CAST(NULL AS bigint) AS data_fine_processo_gdm,
   from_unixtime(unix_timestamp(RCUGAS_PDR_MISURATORE.T_DATA_INST_MISURATORE, 'dd/MM/yyyy')) AS data_inizio_validita_gdm,
   'PRO001' AS id_processo_gdm,
   CASE
      WHEN unix_timestamp(RCUGAS_PDR_MISURATORE.T_DATA_INST_MISURATORE, 'dd/MM/yyyy') >= UNIX_TIMESTAMP() THEN 'true'
      ELSE 'false'
      END AS in_corso_gdm,
   'note' AS note_gdm,
   'cambio_gdm' AS tipo_processo_gdm,
   CAST(NULL AS bigint) AS data_inizio_processo_switch,
   CAST(NULL AS bigint) AS data_fine_processo_switch,
   case when  PRT_SWG.t_codice_pdr is null then null else PRT_SWG.D_DATA_DECORRENZA end  as  data_inizio_validita_switch,
   --PRT_SWG.D_DATA_DECORRENZA AS data_inizio_validita_switch,
   'PRO002' AS id_processo_switch,
   case when PRT_SWG.t_codice_pdr is null then 'false' else 'true' end  as
     in_corso_switch,
   'note' AS note_switch,
   'switch' AS tipo_processo_switch
FROM (
      select rcugas_fornitura.*,Attive.t_nome,
   Attive.t_cognome,
   Attive.t_partita_iva,
   Attive.t_ragione_sociale,
   Attive.t_cf_piva t_codice_fiscale,
   Attive.n_id_cliente AS id,
   cast(case when rcugas_fornitura.d_data_inizio is null then date_format(date_sub(current_date,${env:limit_gg_gas}),'yyyyMMdd') else date_format(rcugas_fornitura.d_data_inizio,'yyyyMMdd') end  as int) inizio_num,
   cast(case when rcugas_fornitura.d_data_fine is null then date_format(current_date,'yyyyMMdd') else date_format(rcugas_fornitura.d_data_fine,'yyyyMMdd') end  as int) fine_num
 from
 (
     Select RCUGAS_CLIENTEFINALE.*,
      case nvl(t_codice_fiscale,'') when '' then t_partita_iva else t_codice_fiscale end t_cf_piva
      from
      ( select
            n_id_cliente
          from rcugas.rcugas_fornitura_p
          where (NVL(d_data_fine,'')) = ''
      ) as fornitura1
         JOIN RCUGAS.RCUGAS_CLIENTEFINALE_p as RCUGAS_CLIENTEFINALE
    ON fornitura1.n_id_cliente= RCUGAS_CLIENTEFINALE.n_id_cliente
    where concat(nvl(t_codice_fiscale,''),nvl(t_partita_iva,'')) <> ''
 ) AS Attive
    join RCUGAS.rcugas_fornitura_p as rcugas_fornitura
 on  rcugas_fornitura.n_id_cliente= Attive.n_id_cliente

 where ( (NVL(rcugas_fornitura.d_data_fine,'')) = ''  or 
 (cast(date_format(rcugas_fornitura.d_data_fine,'yyyyMMdd') as int) >= cast(date_format(date_sub(current_date,${env:limit_gg_gas}),'yyyyMMdd') as int)))
 and NVL(Attive.t_cf_piva,'') <> ''
 ) AS rcugas_fornitura


--residenza
LEFT outer JOIN misuregas.RCUGAS_RESIDENZA ON RCUGAS_RESIDENZA.n_id_fornitura = rcugas_fornitura.n_id_fornitura
LEFT outer JOIN RCUGAS.RCUGAS_PDR_p AS RCUGAS_PDR ON RCUGAS_PDR.n_id_pdr=rcugas_fornitura.n_id_pdr

-- RCUGAS.RCUGAS_PDR_DATIPRELIEVO
left outer JOIN misuregas.rcugas_pdr_datiprelievo on rcugas_pdr_datiprelievo.n_id_pdr= RCUGAS_PDR.n_id_pdr
Left outer join misuregas.RCUGAS_PDR_MISURATORE ON RCUGAS_PDR_MISURATORE.N_ID_PDR=rcugas_fornitura.n_id_pdr

--RCUGAS_INDIRIZZI
LEFT outer JOIN RCUGAS.RCUGAS_INDIRIZZI_p AS RCUGAS_INDIRIZZI ON RCUGAS_INDIRIZZI.n_id=rcugas_fornitura.n_indirizzo_fornitura

-- azienda venditore
LEFT outer JOIN RCUGAS.RCUGAS_VENDITORE_p AS RCUGAS_VENDITORE ON RCUGAS_VENDITORE.n_id_venditore=rcugas_fornitura.n_id_vend

-- azienda venditore
LEFT outer JOIN RCU.V_RCU_AZIENDA_p AS V_RCU_AZIENDA ON V_RCU_AZIENDA.N_ID_AZIENDA =RCUGAS_VENDITORE.n_id_azienda


-- ragione sociale distributore
LEFT outer JOIN misuregas.RCUGAS_CONNESSIONI_DISTR ON RCUGAS_CONNESSIONI_DISTR.t_codice_pdr=RCUGAS_PDR.t_codice_pdr
LEFT outer JOIN misuregas.v_RCUGAS_DISTRIBUTORE ON v_RCUGAS_DISTRIBUTORE.n_id_distributore=RCUGAS_CONNESSIONI_DISTR.n_id_distr

LEFT outer JOIN misuregas.PRT_SWG ON PRT_SWG.t_codice_pdr = RCUGAS_PDR.t_codice_pdr

where NVL(RCUGAS_PDR.t_codice_pdr,'') <> '';


Drop Table misuregas.ProcessiGas;
Create Table misuregas.ProcessiGas Stored As Parquet
as
select distinct forniture_gas.t_codice_pdr,
CAST(NULL AS bigint) AS data_inizio_processo_switch,
   CAST(NULL AS bigint) AS data_fine_processo_switch,
   case when  PRT_SWG.t_codice_pdr is null then null else PRT_SWG.D_DATA_DECORRENZA end  as  data_inizio_validita_switch,
   'PRO002' AS id_processo_switch,
   case when PRT_SWG.t_codice_pdr is null then 'false' else 'true' end  as
     in_corso_switch,
   'note' AS note_switch,
   'switch' AS tipo_processo_switch
From misuregas.forniture_gas
join
(
SELECT
      PRT_SWG.t_codice_pdr,
      MIN(D_DATA_DECORRENZA) AS D_DATA_DECORRENZA
   FROM (select * from SWITCH_GAS.PRT_SWG_p as PRT_SWG where  T_STATO not in ( 'B','TE2','TE3','E1','E2','E3'))PRT_SWG
   WHERE D_DATA_DECORRENZA>= from_unixtime(unix_timestamp())
   GROUP BY t_codice_pdr
) as PRT_SWG
ON PRT_SWG.t_codice_pdr = forniture_gas.t_codice_pdr
where NVL(forniture_gas.t_codice_pdr,'') <> '';
