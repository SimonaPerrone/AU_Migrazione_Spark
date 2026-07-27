package it.eng.au.portale_consumi_ee.model.mongodbs

case class fasceModel(
                       n_id_pod: String = null,
                       n_id_misuratore: String = null,
                       f_lunedi: String = null,
                       f_martedi: String = null,
                       f_mercoledi: String = null,
                       f_giovedi: String = null,
                       f_venerdi: String = null,
                       f_sabato: String = null,
                       f_domenica: String = null,
                       f_festivo: String = null,
                       d_inizio_validita:  java.lang.Long = null,
                       d_fine_validita:  java.lang.Long = null,
                       d_fine_validita_str: String = null,
                       d_data_iniziofreezing: String = null
                         )
