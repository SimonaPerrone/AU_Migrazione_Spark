package it.eng.au.portale_consumi_ee.model.misure

case class misureStoricF2ErcEriModel(
                                      cf_piva: String = null,
                                      pod: String = null,
                                      data_lettura_str: String = null,
                                      data_ricezione: String = null,
                                      lettura_erc_f1: java.lang.Double = null,
                                      lettura_erc_f2: java.lang.Double = null,
                                      lettura_erc_f3: java.lang.Double = null,
                                      lettura_erc_f4: java.lang.Double = null,
                                      lettura_erc_f5: java.lang.Double = null,
                                      lettura_erc_f6: java.lang.Double = null,
                                      lettura_eri_f1: java.lang.Double = null,
                                      lettura_eri_f2: java.lang.Double = null,
                                      lettura_eri_f3: java.lang.Double = null,
                                      lettura_eri_f4: java.lang.Double = null,
                                      lettura_eri_f5: java.lang.Double = null,
                                      lettura_eri_f6: java.lang.Double = null,
                                      erc: String = null,
                                      eri: String = null,
                                      tipo_flusso: String = null,
                                      annomese_riferimento: java.lang.Integer = null,
                                      data_lettura: java.lang.Long = null,
                                      cod_pod: String = null, // char(2) represented as String
                                      is_mis_oraria: String = null // char(1) represented as String
                                    )
