package it.eng.au.ERP.utility.setting

import com.typesafe.config.{Config, ConfigFactory}

object PropertyUtility {

  val config: Config = ConfigFactory.load()

  def flussoMisureInterconnessioneTable: String = config.getString("hive.table.au.flusso_misure_interconnessione")
  def rcuPodPTable: String = config.getString("hive.table.rcu.rcu_pod_p")
  def podConnessioneTable: String = config.getString("hive.table.au.pod_connessione")
  def aggregazioniMisureQuartorarieTable: String = config.getString("hive.table.au.aggregazioni_misure_quartorarie")
  def vAggregazioniMisureQuartorarieTable: String = config.getString("hive.table.au.v_aggregazioni_misure_quartorarie")
  def rcuAziendaPTable: String = config.getString("hive.table.rcu.rcu_azienda_p")
  def rcuPodDistrPTable: String = config.getString("hive.table.rcu.rcu_pod_distr_p")
  def rcusPodDistrTable: String = config.getString("hive.table.rcus.rcus_pod_distr")
  def erpAggregatoOTable: String = config.getString("hive.table.erp.erp_aggregato_o")
  def erpAggregatoIPOTable: String = config.getString("hive.table.erp.erp_aggregato_ip_o")
  def erpAggregatoPub: String = config.getString("hive.table.erp.erp_aggregato_pub")
  def erpAggregato: String = config.getString("hive.table.erp.erp_aggregato")
  def aggregazioneMisureIPTable: String = config.getString("hive.table.au.v_aggregazione_misure_ip")
  def erpValidatedInt: String = config.getString("hive.table.erp.erp_validated_int")
  def erpDettaglioInt: String = config.getString("hive.table.erp.erp_dettaglio_int")
  def erpAggregatoInt: String = config.getString("hive.table.erp.erp_aggregato_int")
  def auFlussoMisureSmis: String = config.getString("hive.table.au.flusso_misure_smis")
  def auFlussoMisureNoAggr: String = config.getString("hive.table.au.flusso_misure_noaggr")
  def auFlussitecnici: String = config.getString("hive.table.au.flussi_tecnici")
  def rcuPodInterconnesioneP: String = config.getString("hive.table.rcu.rcu_pod_interconnessione_p")
  def erpValidatedMisNo: String = config.getString("hive.table.erp.erp_validated_mis_no")
  def trattPodAllAnnomesePartitined: String = config.getString("hive.table.tratt_pod.tratt_pod_all_annomese_partitioned")
  def erpConsumptionNo: String = config.getString("hive.table.erp.erp_consumption_no")
  def erpDailyDima: String = config.getString("hive.table.erp.erp_daily_dima")
  def erpDailyNo: String = config.getString("hive.table.erp.erp_daily_no")
  def erpAggregatoNo: String = config.getString("hive.table.erp.erp_aggregato_no")
  def erpDetPodIntPub: String = config.getString("hive.table.erp.erp_det_pod_int_pub")
}
