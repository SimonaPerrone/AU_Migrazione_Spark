package it.eng.au.ccgPubblicazione.schema.request

import it.eng.au.ccgPubblicazione.schema.SchemaEnum

object RequestPdrSchema extends SchemaEnum {
  val
  N_ID_RICHIESTA,
  T_SERVIZIO,
  T_PROCESSO,
  T_ANNO,
  T_MESE,
  T_RUOLO,
  T_PIVA,

  T_CODICE_PDR,

  B_AMMISSIBILITA,
  T_COD_CAUSALE,
  T_MOTIVAZIONE,

  T_NOME_FILE,
  T_TIPO_AMM,
  PARTITION_REQUEST_DATE
  = Value
}
