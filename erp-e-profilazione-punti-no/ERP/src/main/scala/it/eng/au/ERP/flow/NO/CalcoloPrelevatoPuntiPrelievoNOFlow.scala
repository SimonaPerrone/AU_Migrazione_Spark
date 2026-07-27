package it.eng.au.ERP.flow.NO

import it.eng.au.ERP.dao.hive.erp.{ErpAggregatoNoDao, ErpDailyNoDao}
import it.eng.au.ERP.flow.Flow
import it.eng.au.ERP.trasformations.NO.CalcoloPrelevatoPuntiPrelievoNOTransformation
import org.apache.spark.sql.SparkSession

case class CalcoloPrelevatoPuntiPrelievoNOFlow()(implicit spark: SparkSession) extends Flow {

  // Input DAO
  private val erpDailyNoDao = new ErpDailyNoDao

  // Output DAO
  private val erpAggregatoNoDao = new ErpAggregatoNoDao

  /**
   * Executes the final aggregation step for Non-Orari (NO) flow
   * 
   * Process:
   * 1. Read erp_daily_no (output from profilazione step)
   * 2. Aggregate by area/PIVA/day summing all quartori
   * 3. Write to erp_aggregato_no
   * 
   * @param timestamp Execution ID for tracking
   * @param podExcluded List of PODs to exclude from aggregation
   * @param annomese Optional year-month filter (e.g., "202501")
   */
  def run(timestamp: Long, podExcluded: List[String], annomese: Option[String]): Unit = {

    logger.info("Inizio aggregazione finale punti prelievo NO (Non Orari)")

    // Read daily profiled data
    val dfDailyNo = erpDailyNoDao.read()

    logger.info(s"Lettura dati da ${erpDailyNoDao.tableName} completata")

    // Aggregate daily profiles by area/PIVA/day
    val aggregatedDf = CalcoloPrelevatoPuntiPrelievoNOTransformation.aggregateNODailyProfiles(
      dfDailyNo,
      timestamp,
      podExcluded
    )

    logger.info(s"Aggregazione completata, righe generate: ${aggregatedDf.count()}")

    // Write aggregated data to erp_aggregato_no
    logger.info(s"Inizio scrittura aggregazione NO su tabella ${erpAggregatoNoDao.tableName}")
    erpAggregatoNoDao.write(aggregatedDf, overwrite = false)
    logger.info(s"Fine scrittura aggregazione NO su tabella ${erpAggregatoNoDao.tableName}")

    logger.info("Fine aggregazione finale punti prelievo NO (Non Orari)")
  }
}
