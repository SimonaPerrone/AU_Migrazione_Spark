package it.eng.au.ccgPubblicazione.schema.request

import it.eng.au.ccgPubblicazione.schema.SchemaEnum

object RequestFilterSchema extends SchemaEnum {
  val
  N_ID_RICHIESTA,
  T_SERVIZIO,
  T_PROCESSO,
  T_ANNO,
  T_MESE,
  T_RUOLO,
  T_PIVA,

  T_COD_REMI,
  T_INCOERENTI,
  T_TRATTAMENTO,
  T_PIVA_UDD,
  T_PIVA_UDB,
  T_PIVA_ID,
  T_CODPROFSTD,
  PARTITION_REQUEST_DATE
  = Value
}
