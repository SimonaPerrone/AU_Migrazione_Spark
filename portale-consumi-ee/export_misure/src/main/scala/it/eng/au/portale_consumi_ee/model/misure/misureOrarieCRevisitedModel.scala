package it.eng.au.portale_consumi_ee.model.misure

case class misureOrarieCRevisitedModel(
                               n_id_fornitura: String = null,
                               pod: String = null,
                               competenza_consumi: java.lang.Integer = null,
                               misura_oraria_gg:  List[misureOrarieCStructValues] = null,
                               misura_oraria_gg_hashed: String = null
                                      )

case class misureOrarieCRevisitedModelSingleElement(
                                        giorno: java.lang.Integer = null,
                                        n_id_fornitura: String = null,
                                        pod: String = null,
                                        competenza_consumi: java.lang.Integer = null,
                                        misura_oraria_gg_element:  misureOrarieCStructValues = null
                                      )

case class misureOrarieCStructValues(
                                      giorno: String = null,
                                      competenza_consumi: String = null,
                                      consumo_giornaliero_gg: String = null,
                                      lettura_misura_f1: String = null,
                                      lettura_misura_f2: String = null,
                                      lettura_misura_f3: String = null,
                                      lettura_misura_f4: String = null,
                                      lettura_misura_f5: String = null,
                                      lettura_misura_f6: String = null,
                                      delta_misure_f1: String = null,
                                      delta_misure_f2: String = null,
                                      delta_misure_f3: String = null,
                                      delta_misure_f4: String = null,
                                      delta_misure_f5: String = null,
                                      delta_misure_f6: String = null,
                                      potenza_max_erogata: String = null,
                                      tipo_misura: String = null,
                                      data_lettura: String = null
                                    )

