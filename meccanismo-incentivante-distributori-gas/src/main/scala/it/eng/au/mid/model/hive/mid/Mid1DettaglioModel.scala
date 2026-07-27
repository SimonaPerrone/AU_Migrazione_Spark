package it.eng.au.mid.model.hive.mid

// Definizione MID1 ed utilizzato anche per calcolo MID2
case class Mid1DettaglioModel(
                               pdr: String = null,
                               contatore: Int = 0,
                               piva_id: String = null,
                               piva_udd: String = null,
                               cod_remi: String = null,
                               gdm: String = null,
                               alpha: Int = 0,
                               executionid_mid_contatori: Long = 0L,
                               annomese: String = null,
                               executionid: Long = 0L
                             )
