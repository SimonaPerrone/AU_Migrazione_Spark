package it.au.misure.calcolo_capacita.component.utility.property

import com.typesafe.config.ConfigFactory
import it.au.misure.calcolo_capacita.component.contract.Property

object RunningProperty extends Property {

  lazy private val config = ConfigFactory.load
  lazy val parallelism: Int = config.getInt("running.parallelism")
  lazy val shuffle: Int = config.getInt("running.shuffle")
}
