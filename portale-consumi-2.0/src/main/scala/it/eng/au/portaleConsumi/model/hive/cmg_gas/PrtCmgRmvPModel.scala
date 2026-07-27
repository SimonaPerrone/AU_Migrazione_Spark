package it.eng.au.portaleConsumi.model.hive.cmg_gas

import java.sql.Timestamp


case class PrtCmgRmvPModel(
                            n_id_file: String = null,
                            t_name_file: String = null,
                            annomese_riferimento: String = null,
                            cod_servizio: String = null,
                            cod_flusso: String = null,
                            d_caricamento: Timestamp = null,
                            cod_pdr: String = null,
                            cod_prat_attivazione: String = null,
                            matr_mis: String = null,
                            matr_conv: String = null,
                            coeff_corr: String = null,
                            progr_anno_term: String = null,
                            let_tot_prel: Integer = null,
                            let_tot_conv: String = null,
                            mot_rett_lett: String = null,
                            data_comp: String = null,
                            anno: String = null,
                            mese: String = null,
                            piva_distr: String = null,
                            piva_utente: String = null,
                            local_file: String = null,
                            mese_comp: String = null,
                            tipo_rettifica: String = null,
                            data_prest: String = null,
                            codprat_sii: String = null,
                            trattamento: String = null,
                            freq_let: String = null,
                            vol_annuo_rettificato: String = null,
                            data_racc: Timestamp = null,
                            vol_ric: String = null,
                            ini_periodo: String = null,
                            fine_periodo: String = null,
                            periodo_ric: String = null,
                            ammissibilita: String = null,
                            causa_ostativa: String = null,
                            annomese: String = null
  )
