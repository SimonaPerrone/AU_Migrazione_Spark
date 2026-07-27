package it.eng.au.portale_consumi_ee.model.misure

case class autoletturaRevisitedModel(
                              n_id_fornitura:String = null,
                              pod: String = null,
                              competenza_consumi: java.lang.Integer = null,
                              autolettura : AutoletturaValues = null,
                              autolettura_hashed: String = null
                           )

case class AutoletturaValues (
                             competenza_consumi: String = null,
                             data_lettura: String = null,
                             lettura_misura_monoraria: String = null,
                             lettura_misura_f1: String = null,
                             lettura_misura_f2: String = null,
                             lettura_misura_f3: String = null,
                             lettura_misura_f4: String = null,
                             lettura_misura_f5: String = null,
                             lettura_misura_f6: String = null
                             )

case class autolettureHashed(
                             cf_piva: String = null,
                             n_id_fornitura:String = null,
                             pod: String = null,
                             competenza_consumi: java.lang.Integer = null,
                             data_lettura: java.lang.Long = null,
                             lettura_misura_monoraria: java.lang.Double = null,
                             lettura_misura_f1: java.lang.Double = null,
                             lettura_misura_f2: java.lang.Double = null,
                             lettura_misura_f3: java.lang.Double = null,
                             lettura_misura_f4: java.lang.Double = null,
                             lettura_misura_f5: java.lang.Double = null,
                             lettura_misura_f6: java.lang.Double = null,
                             autolettura_hashed: String = null
                           )