package it.au.misure.ingestionMisureGasUnico.model.schema

object DecompressioneLogSchema extends SchemaEnum {
  val STATUS_0: DecompressioneLogSchema.Value = Value("000")
  val STATUS_1: DecompressioneLogSchema.Value = Value("001")
  val STATUS_2: DecompressioneLogSchema.Value = Value("002")
  val STATUS_3: DecompressioneLogSchema.Value = Value("003")
  val STATUS_4: DecompressioneLogSchema.Value = Value("004")
  val STATUS_5: DecompressioneLogSchema.Value = Value("005")
  val STATUS_6: DecompressioneLogSchema.Value = Value("006")
  val STATUS_7: DecompressioneLogSchema.Value = Value("007")

  val PHASE_U: DecompressioneLogSchema.Value = Value("U")
  val PHASE_C: DecompressioneLogSchema.Value = Value("C")
  val PHASE_E: DecompressioneLogSchema.Value = Value("E")
}
