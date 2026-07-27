package it.eng.au.portale_consumi_ee.utility.setting

import com.typesafe.config.{Config, ConfigFactory}

object PropertyUtility {
  val config: Config = ConfigFactory.load()

  def autolettureTable: String = config.getString("hive.table.misure.autoletture")
  def misureMensiliCTable: String = config.getString("hive.table.misure.misure_mensili_c")

  def misureNonOrarieCTable: String = config.getString("hive.table.misure.misure_non_orarie_c")
  def misureOrarieCTable: String = config.getString("hive.table.misure.misure_orarie_c")
  def voltureTable: String = config.getString("hive.table.misure.volture")
  def etldStageg3M2Table: String = config.getString("hive.table.misure.etl_stage3m_2")
  def etldStageg33M2Table: String = config.getString("hive.table.misure.etl_stage33m_2")
  def registroLoadTable: String = config.getString("hive.table.misure.registro_load")
  def ambiente_lavoro : String = config.getString("ambiente.lavoro")


}
