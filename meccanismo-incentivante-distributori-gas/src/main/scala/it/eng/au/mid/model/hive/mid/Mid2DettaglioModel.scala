package it.eng.au.mid.model.hive.mid

case class Mid2DettaglioModel(
                               pdr: String = null,
                               contatore: Int = 0,
                               piva_id: String = null,
                               rag_soc_id: String = null,
                               stato_id: String = null,
                               piva_udd: String = null,
                               rag_soc_udd: String = null,
                               piva_distr_att: String = null,
                               rag_soc_distr_att: String = null,
                               cod_remi: String = null,
                               gdm: String = null,
                               alpha: Int = 0,
                               executionid_mid_contatori: Long = 0L,
                               annomese: String = null,
                               executionid: Long = 0L
                             )
