package it.eng.au.portale_consumi_ee.model.mongodbs

case class GdmModel(
                        n_id_pod: String = null,
                        codice_pod: String = null,
                        n_potenza_disponibile: String = null,
                        n_potenza_impegnata: String = null,
                        n_tensione: String = null,
                        t_tipo_misuratore: String = null,
                        d_oper_misurator_att:  java.lang.Long = null,
                        d_oper_misurator_att_str: String = null,
                        cambio_gdm: String = null,
                        data_cambio_gdm:  java.lang.Long = null,
                        data_cambio_gdm_str: String = null,
                        trattamento: String = null,
                        stato_misuratore_2g: String = null,
                        t_mat_misuratore_att: String = null,
                        d_inst_misurator_att:  java.lang.Long = null,
                        anno_start_misure_orarie:  java.lang.Integer = null,
                        mese_start_misure_orarie:  java.lang.Integer = null
                         )
