package it.eng.au.pubblicazioneIndennizzi.model

case class DettaglioPDR(
                         id_indennizzo: Long = 0L,
                         piva_id: String = "",
                         rag_soc_id: String = "",
                         piva_udd: String = "",
                         rag_soc_udd: String = "",
                         pdr: String = "",
                         nome_file: String = "",
                         annomese: String = "",
                         executionid: Long = 0L
                       )