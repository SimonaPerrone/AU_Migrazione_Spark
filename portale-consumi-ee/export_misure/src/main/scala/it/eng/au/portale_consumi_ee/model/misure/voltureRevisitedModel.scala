package it.eng.au.portale_consumi_ee.model.misure

case class voltureRevisitedModel(
                         n_id_fornitura: String = null,
                         pod: String = null,
                         competenza_consumi: java.lang.Integer = null,
                         volture: VoltureValues= null,
                         volture_hashed: String = null
                                )

case class VoltureValues (
                           competenza_consumi: String = null,
                           data_lettura: String = null,
                           lettura_misura_monoraria: String = null,
                           lettura_misura_f1: String = null,
                           lettura_misura_f3: String = null,
                           lettura_misura_f4: String = null,
                           lettura_misura_f5: String = null,
                           lettura_misura_f6: String = null,
                           tipo_misura: String = null
                         )

case class voltureHashedModel(
                         cf_piva: String = null,
                         n_id_fornitura: String = null,
                         pod: String = null,
                         tipo_flusso2: String = null,
                         competenza_consumi: java.lang.Integer = null,
                         data_lettura: java.lang.Long = null,
                         lettura_misura_monoraria: java.lang.Double = null,
                         lettura_misura_f1: java.lang.Double = null,
                         lettura_misura_f2: java.lang.Double = null,
                         lettura_misura_f3: java.lang.Double = null,
                         lettura_misura_f4: java.lang.Double = null,
                         lettura_misura_f5: java.lang.Double = null,
                         lettura_misura_f6: java.lang.Double = null,
                         volture_hashed: String = null
                             )