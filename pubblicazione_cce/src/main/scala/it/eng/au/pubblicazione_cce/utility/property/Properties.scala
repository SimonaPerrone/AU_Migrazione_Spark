package it.eng.au.pubblicazione_cce.utility.property

import com.typesafe.config.{Config, ConfigFactory}

object Properties {
  private var config: Config = ConfigFactory.load

  def setProperty(key: String, value: String): Unit = config = ConfigFactory.parseString(s"$key=$value").withFallback(config)

}
