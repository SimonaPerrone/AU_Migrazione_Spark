package it.eng.au.aggiustamentoGas.schema.agg

import it.eng.au.aggiustamentoGas.schema.SchemaEnum

object DailyConsumptionIncoerentiGdMSchema extends SchemaEnum {
  val
  pdr,
  date,
  value,
  coefficient,
  annoMese,
  session,
  classeMisuratore,
  valueNotSterilized,
  isDayAnomalous,
  isPdrAnomalousGDM,
  gdmCoefficient
  = Value
}
