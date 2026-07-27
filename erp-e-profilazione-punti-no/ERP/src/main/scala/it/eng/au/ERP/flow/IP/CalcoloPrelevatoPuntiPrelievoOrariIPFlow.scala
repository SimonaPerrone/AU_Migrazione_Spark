package it.eng.au.ERP.flow.IP

import it.eng.au.ERP.dao.hive.au.VAggregazioneMisureIPDao
import it.eng.au.ERP.dao.hive.erp.ErpAggregatoIPODao
import it.eng.au.ERP.dao.hive.rcu.RcuAziendaPDao
import it.eng.au.ERP.flow.Flow
import it.eng.au.ERP.trasformations.IP.CalcoloPrelevatoPuntiPrelievoOrariIPTrasformation
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.functions.argumentsUtilities
import org.apache.spark.sql.SparkSession

case class CalcoloPrelevatoPuntiPrelievoOrariIPFlow(implicit spark: SparkSession) extends Flow {

  //input dao
  val vAggregazioneMisureIPDao = new VAggregazioneMisureIPDao
  val rcuAziendaPDao = new RcuAziendaPDao

  //output dao
  val erpAggregatoIPODao = new ErpAggregatoIPODao

  def run(timestamp: Long, podExcluded: List[String], args: ERPArgsConfig
          , area: Option[String], annomese: Option[String], singola_piva_distributore: Option[String]): Unit = {

    val (year, month)
    : (Option[Int], Option[Int]) = argumentsUtilities.yearMonth(annomese) match {
      case Some((year, month)) => (Some(year), Some(month))
      case None => (None, None)
    }

    //dataset input
    val dsvAggregazioneMisureIP = vAggregazioneMisureIPDao.read()
    val dsRcuAziendaP = rcuAziendaPDao.read()

    logger.info("Inizio Calcolo Prelevato ai punti di prelievo IP (NON ORARI)")
    val finalDf = CalcoloPrelevatoPuntiPrelievoOrariIPTrasformation.calcoloPrelevatoPuntiPrelievoOrariIP(
      dsvAggregazioneMisureIP,
      dsRcuAziendaP,
      args,
      podExcluded,
      timestamp,
      year,
      month,
      annomese,
      area,
      singola_piva_distributore
    )
    logger.info("Fine Calcolo Prelevato ai punti di prelievo IP (NON ORARI)")

    logger.info(s"Inizio scrittura dati IP (NON ORARI) su tabella ${erpAggregatoIPODao.tableName}")
    //write operation
    erpAggregatoIPODao.write(finalDf, true)
    logger.info(s"Fine scrittura dati IP (NON ORARI) su tabella ${erpAggregatoIPODao.tableName}")


  }

}
