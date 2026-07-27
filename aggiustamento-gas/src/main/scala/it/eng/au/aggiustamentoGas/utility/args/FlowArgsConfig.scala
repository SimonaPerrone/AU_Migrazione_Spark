package it.eng.au.aggiustamentoGas.utility.args

import org.joda.time.DateTime

case class FlowArgsConfig(
                         pathToProperties: String = null,
                         session: String = "AGG",
                         dateToRun: Option[DateTime] = None
                         )
