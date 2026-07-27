package it.eng.au.portale_consumi_ee.model.misure

case class misureOrarieCModel(
                               cf_piva: String = null,
                               n_id_fornitura: String = null,
                               pod: String = null,
                               giorno: java.lang.Integer = null,
                               tipo_misura: String = null,
                               potenza_max_erogata: java.lang.Double = null,
                               lettura_giornaliero_f1: java.lang.Double = null,
                               lettura_giornaliero_f2: java.lang.Double = null,
                               lettura_giornaliero_f3: java.lang.Double = null,
                               lettura_giornaliero_f4: java.lang.Double = null,
                               lettura_giornaliero_f5: java.lang.Double = null,
                               lettura_giornaliero_f6: java.lang.Double = null,
                               delta_misure_f1: java.lang.Double = null,
                               delta_misure_f2: java.lang.Double = null,
                               delta_misure_f3: java.lang.Double = null,
                               delta_misure_f4: java.lang.Double = null,
                               delta_misure_f5: java.lang.Double = null,
                               delta_misure_f6: java.lang.Double = null,
                               consumo_giornaliero_gg: java.lang.Double = null,
                               is2g:  String = null,  // Changed to Char(1)
                               tipo_flusso: String = null,
                               data_lettura: java.lang.Long = null,
                               competenza_consumi: java.lang.Integer = null
                              )
