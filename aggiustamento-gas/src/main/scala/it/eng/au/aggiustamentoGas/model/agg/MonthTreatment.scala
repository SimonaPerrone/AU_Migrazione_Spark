package it.eng.au.aggiustamentoGas.model.agg

case class MonthTreatment(
                         pdr: String,
                         month: String, //format yyyyMM
                         treatment: String,
                         calcmode: String,
                         autofilled: Boolean
                         )
