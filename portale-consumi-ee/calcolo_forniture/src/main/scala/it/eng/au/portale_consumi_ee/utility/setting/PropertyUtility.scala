package it.eng.au.portale_consumi_ee.utility.setting

import com.typesafe.config.{Config, ConfigFactory}

object PropertyUtility {

  val config: Config = ConfigFactory.load()

  def job_properties: String = config.getString("deploy.app.properties")

}
