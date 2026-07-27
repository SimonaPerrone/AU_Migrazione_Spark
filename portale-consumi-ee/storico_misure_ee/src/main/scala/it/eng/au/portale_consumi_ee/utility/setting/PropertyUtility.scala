package it.eng.au.portale_consumi_ee.utility.setting

import com.typesafe.config.{Config, ConfigFactory}

object PropertyUtility {
  val config: Config = ConfigFactory.load()

  def fornitureElettricheTmp: String = config.getString("hive.table.mongodbs.forniture_elettriche_tmp")
  def fornitureElettriche: String = config.getString("hive.table.mongodbs.forniture_elettriche")
  def misureStoric: String = config.getString("hive.table.misure.misure_storic")
  def misureStoricNora: String = config.getString("hive.table.misure.misure_storic_nora")
  def misureStoricF: String = config.getString("hive.table.misure.misure_storic_f")
  def misureStoricF2: String = config.getString("hive.table.misure.misure_storic_f2")

  def misureStoricF2ErcEri:String =  config.getString("hive.table.misure.misure_storic_f2_erc_eri")
  def misureStoricNoraErcEri:String =  config.getString("hive.table.misure.misure_storic_nora_erc_eri")
  def consultazione: String = config.getString("hive.table.misure.consultazione")
}

