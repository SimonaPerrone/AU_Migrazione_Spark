package it.eng.au.pubblicazioneRendiconti.schema

import it.eng.au.indennizziMisureGasCommon.schema.SchemaEnum

object Rzg2CsvOutputSchema extends SchemaEnum{
  val DATA,
  ID_INDENNIZZO,
  PIVA_ID,
  RAG_SOC_ID,
  PIVA_UDD,
  RAG_SOC_UDD,
  _EURO_SYMBOL_OM1_ID,
  _EURO_SYMBOL_OM2_ID,
  _EURO_SYMBOL_OM3_ID
  = Value
}
