package it.eng.au.portale_consumi_ee.model.misure

case class misureMensiliCModel(
                             cf_piva: String = null,
                             n_id_fornitura: String = null,
                             tipo_misura: String = null,
                             lettura_misura_monoraria: java.lang.Integer = null,
                             delta_misura_monoraria: java.lang.Double = null,
                             lettura_misura_f1: java.lang.Double = null,
                             lettura_misura_f2: java.lang.Double = null,
                             lettura_misura_f3: java.lang.Double = null,
                             lettura_misura_f4: java.lang.Double = null,
                             lettura_misura_f5: java.lang.Double = null,
                             lettura_misura_f6: java.lang.Double = null,
                             delta_misure_f1: java.lang.Double = null,
                             delta_misure_f2: java.lang.Double = null,
                             delta_misure_f3: java.lang.Double = null,
                             delta_misure_f4: java.lang.Double = null,
                             delta_misure_f5: java.lang.Double = null,
                             delta_misure_f6: java.lang.Double = null,
                             pod:String = null,
                             tipo_flusso:String = null,
                             data_lettura: java.lang.Long = null,
                             competenza_consumi: java.lang.Integer = null,
                             da_antswitch:  String = null
                              )
