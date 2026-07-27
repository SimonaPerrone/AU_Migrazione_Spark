package it.eng.au.ERP.flow.TERNA

import it.eng.au.ERP.dao.hive.au.{PodConnessioneDao, vAggregazioniMisureQuartorarieDao}
import it.eng.au.ERP.dao.hive.erp.ErpAggregatoODao
import it.eng.au.ERP.dao.hive.rcu.RcuAziendaPDao
import it.eng.au.ERP.flow.Flow
import it.eng.au.ERP.trasformations.TERNA.CalcoloPrelevatoPuntiPrelievoOrariTERNATrasformation
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.functions.argumentsUtilities
import org.apache.spark.sql.SparkSession

case class CalcoloPrelevatoPuntiPrelievoOrariTERNAFlow(implicit spark: SparkSession) extends Flow {

  //input dao
  val vAggregazioniMisureQuartorarieDao = new vAggregazioniMisureQuartorarieDao
  val rcuAziendaPDao = new RcuAziendaPDao
  val podConnessioneDao = new PodConnessioneDao

  //output dao
  val erpAggregatoODao = new ErpAggregatoODao

  def run(timestamp: Long, podExcluded: List[String], args: ERPArgsConfig, terna: Boolean
          , area: Option[String], annomese: Option[String], singola_piva_distributore: Option[String]): Unit = {

    val (year, month)
    : (Option[Int], Option[Int]) = argumentsUtilities.yearMonth(annomese) match {
      case Some((year, month)) => (Some(year), Some(month))
      case None => (None, None)
    }

    //dataset input
    val dsAggregazioniMisureQuartorarie = vAggregazioniMisureQuartorarieDao.read()
    val dsRcuAziendaP = rcuAziendaPDao.read()
    val dsPodConnessione = podConnessioneDao.read()


    //note
    //currentPartitions can be avoided and decide to tspark the number of partition based
    //on cluster cores
    val currentPartitions = dsAggregazioniMisureQuartorarie.rdd.getNumPartitions

    logger.info("Inizio Calcolo Prelevato ai punti di prelievo ORARI (TERNA)")
    val finalDf = CalcoloPrelevatoPuntiPrelievoOrariTERNATrasformation.calcoloPrelevatoPuntiPrelievoOrari(
      dsAggregazioniMisureQuartorarie,
      dsRcuAziendaP,
      dsPodConnessione,
      args,
      currentPartitions,
      podExcluded,
      timestamp,
      terna,
      year,
      month,
      area,
      singola_piva_distributore
    )
    logger.info("Fine Calcolo Prelevato ai punti di prelievo ORARI (TERNA)")


    logger.info(s"Inizio scrittura dati ORARI (TERNA) su tabella ${erpAggregatoODao.tableName}")
    erpAggregatoODao.write(finalDf, false)
    logger.info(s"Fine scrittura dati ORARI (TERNA) su tabella ${erpAggregatoODao.tableName}")


  }

}
