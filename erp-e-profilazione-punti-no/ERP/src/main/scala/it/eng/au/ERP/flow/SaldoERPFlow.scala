package it.eng.au.ERP.flow

import it.eng.au.ERP.dao.hive.erp.{ErpAggregatoDao, ErpAggregatoIntDao, ErpAggregatoIPODao, ErpAggregatoNoDao, ErpAggregatoODao}
import it.eng.au.ERP.trasformations.SaldoERPAggregationTransformation
import it.eng.au.ERP.utility.functions.argumentsUtilities
import org.apache.spark.sql.SparkSession

case class SaldoERPFlow()(implicit spark: SparkSession) extends Flow {

  private val erpAggregatoIntDao = new ErpAggregatoIntDao
  private val erpAggregatoODao = new ErpAggregatoODao
  private val erpAggregatoNoDao = new ErpAggregatoNoDao
  private val erpAggregatoIPODao = new ErpAggregatoIPODao
  private val erpAggregatoDao = new ErpAggregatoDao

  def run(timestamp: Long, annomese: Option[String]): Unit = {
    val (yearOpt, monthOpt) = argumentsUtilities.yearMonth(annomese) match {
      case Some((y, m)) => (Some(y), Some(m))
      case None => (None, None)
    }

    if (yearOpt.isEmpty || monthOpt.isEmpty) {
      logger.warn("SaldoERPFlow: annomese non valorizzato; saldo non eseguito")
      return
    }
    val year = yearOpt.get; val month = monthOpt.get

    logger.info("Ricerca max executionid per mese (INT, O, NO, IP)")
    val execInt = erpAggregatoIntDao.latestExecutionIdForMonth(year, month)
    val execO = erpAggregatoODao.latestExecutionIdForMonth(year, month)
    val execNo = erpAggregatoNoDao.latestExecutionIdForMonth(year, month)
    val execIp = erpAggregatoIPODao.latestExecutionIdForMonth(year, month)

    logger.info(s"ExecutionId selezionati: INT=${execInt.getOrElse("none")}, O=${execO.getOrElse("none")}, NO=${execNo.getOrElse("none")}, IP=${execIp.getOrElse("none")}")

    logger.info("Lettura tabelle per anno/mese con executionid selezionato")
    val dfInt = execInt.map(erpAggregatoIntDao.readForMonthAndExecutionId(year, month, _))
      .getOrElse(spark.table(erpAggregatoIntDao.tableName).limit(0).selectExpr(erpAggregatoIntDao.columns: _*))
    val dfO = execO.map(erpAggregatoODao.readForMonthAndExecutionId(year, month, _))
      .getOrElse(spark.table(erpAggregatoODao.tableName).limit(0).selectExpr(erpAggregatoODao.columns: _*))
    val dfNo = execNo.map(erpAggregatoNoDao.readForMonthAndExecutionId(year, month, _))
      .getOrElse(spark.table(erpAggregatoNoDao.tableName).limit(0).selectExpr(erpAggregatoNoDao.columns: _*))
    val dfIp = execIp.map(erpAggregatoIPODao.readForMonthAndExecutionId(year, month, _))
      .getOrElse(spark.table(erpAggregatoIPODao.tableName).limit(0).selectExpr(erpAggregatoIPODao.columns: _*))

    logger.info("Calcolo saldo ERP (INT - O - NO - IP)")
    val saldoDf = SaldoERPAggregationTransformation.computeSaldo(
      dfInt, dfO, dfNo, dfIp, year, month, timestamp
    )

    if (saldoDf.head(1).isEmpty) {
      logger.warn("SaldoERPFlow: nessun dato prodotto per il mese selezionato")
      return
    }

    logger.info(s"Scrittura saldo ERP su ${erpAggregatoDao.tableName}")
    erpAggregatoDao.write(saldoDf, overwrite = false)
  }
}
