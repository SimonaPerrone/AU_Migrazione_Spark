package it.sferanet.au.utilities.args

import org.joda.time.DateTime

case class FlowArgsConfig(
                         pathToProperties: String = null,
                         session: String = "CDP",
                         dateToRun: Option[DateTime] = None
                         )
