package it.eng.au.ammissibilitaSettlementGas.schema

// è valido sia per i file TFC (File "TFC_AMM")
// che per i file VPG (File "VPG_AMM"), in quanto condividono lo stesso schema.

object AmmissibilitaCsvSchema extends SchemaEnum {
  val NUM_RIGA,
  COD_TIPO_FILE,
  PIVA_UTENTE,
  VERIFICA_AMM,
  COD_CAUSALE,
  MOTIVAZIONE
  = Value
}
