package it.eng.au.portaleConsumi.model.hive.rcugas

import java.sql.Timestamp

case class RcugasPdrDatiprelievoPModel(
                                        n_id_pdr_datiprelievo: String = null,
                                        n_id_pdr: String = null,
                                        t_anno: String = null,
                                        t_cod_profilo: String = null,
                                        n_prelievo_annuo: String = null,
                                        n_lettura_convertitore: String = null,
                                        t_cod_cat_uso: String = null,
                                        t_cod_classe_prelievo: String = null,
                                        t_note: String = null,
                                        d_aggiornamento: Timestamp = null,
                                        n_id_traccia: String = null,
                                        n_id_s_prec: String = null,
                                        d_data_rif: Timestamp = null,
                                        t_anno_mese_rif: String = null,
                                        t_fattore_correz_climatica: String = null,
                                        t_trattamento_settlement: String = null
                                      )
