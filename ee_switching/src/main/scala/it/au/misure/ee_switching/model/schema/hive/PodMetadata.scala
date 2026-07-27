package it.au.misure.ee_switching.model.schema.hive

case class PodMetadata(
                         nomeFlusso: String = null,
                         trattamento: String = null,
                         tipoMisuratore: String = null,
                         messaRegime: String = null,
                         podRiconfigurato: Boolean = false
                       )

