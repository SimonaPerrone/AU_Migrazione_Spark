package it.eng.au.calcoloIndennizzi.utility.args

case class Args(
                 propertiesPath: String = null,
                 recoveryMode: Boolean = false,
                 yearMonth: Option[String] = None,
                 thresholdDay: Option[String] = None
               )