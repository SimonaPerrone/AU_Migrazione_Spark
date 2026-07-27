package it.eng.au.freezerPreCalcolo.utility.args

import org.joda.time.DateTime

case class FlowArgsConfig(
                         pathToProperties: String = null,
                         session: String = "CDP",
                         dateToRun: Option[DateTime] = None
                         )
