package it.au.misure.calcolo_capacita.component.utility.property

import com.typesafe.config.ConfigFactory
import it.au.misure.calcolo_capacita.component.contract.Property


object ApplicationProperty extends Property {

  lazy private val config = ConfigFactory.load
  lazy val format: String = config.getString("application.date.format")
  lazy val dateFormatToExport: String = config.getString("application.date.format_to_export")
}
