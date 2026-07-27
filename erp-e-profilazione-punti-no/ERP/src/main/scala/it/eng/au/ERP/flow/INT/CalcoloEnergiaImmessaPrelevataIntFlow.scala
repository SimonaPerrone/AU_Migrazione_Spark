package it.eng.au.ERP.flow.INT

import it.eng.au.ERP.dao.hive.au.flussoMisureInterconnessioneDao
import it.eng.au.ERP.dao.hive.erp.{ErpAggregatoIntDao, ErpDettaglioIntDao, ErpValidatedIntDao}
import it.eng.au.ERP.dao.hive.rcu.{RcuAziendaPDao, RcuPodInterconnesionePDao}
import it.eng.au.ERP.flow.Flow
import it.eng.au.ERP.trasformations.INT.CalcoloPrelevatoPuntiPrelievoOrariINTTrasformation
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.functions.argumentsUtilities
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import it.eng.au.ERP.schema.erp.{erpDettaglioINTSchema, erpValidatedIntSchema}

case class CalcoloEnergiaImmessaPrelevataIntFlow(implicit spark: SparkSession) extends Flow {

  //input dao
  val flussoMisureInterconnessioneDao = new flussoMisureInterconnessioneDao
  val rcuPodInterconnesionPDao = new RcuPodInterconnesionePDao
  val rcuAziendaPDao = new RcuAziendaPDao


  // output dao
  val erpValidatedIntDao = new ErpValidatedIntDao
  val erpAggregatoIntDao = new ErpAggregatoIntDao
  val erpDettaglioIntDao = new ErpDettaglioIntDao

  def run(timestamp: Long, podExluded: List[String], args: ERPArgsConfig
          , area: Option[String], annomese: Option[String], singola_piva_distributore: Option[String]): Unit = {

    val (year, month)
    : (Option[Int], Option[Int]) = argumentsUtilities.yearMonth(annomese) match {
      case Some((year, month)) => (Some(year), Some(month))
      case None => (None, None)
    }

    //dataset input
    val dsFlussoMisureInterconnessione = flussoMisureInterconnessioneDao.read()
    val dsrcuPodInterconnesioneP = rcuPodInterconnesionPDao.read()
    val rcuAziendaP = rcuAziendaPDao.read()

    //note
    //currentPartitions can be avoided and decide to tspark the number of partition based
    //on cluster cores
    val currentPartitions = dsFlussoMisureInterconnessione.rdd.getNumPartitions

    logger.info("Inizio Calcolo Saldo Energetico ai Punti di Interconnesione")

    logger.info("Inizio calcolo ingestion flussi di misura Int")
    //ingestion dei flussi di misura int
    val erpValidationIntDf = CalcoloPrelevatoPuntiPrelievoOrariINTTrasformation.calcoloErpValidatedInt(
      dsFlussoMisureInterconnessione,
      dsrcuPodInterconnesioneP,
      args,
      currentPartitions,
      podExluded,
      timestamp,
      year,
      month,
      area,
      singola_piva_distributore
    )
    logger.info("Fine Calcolo ingestion flussi di misura Int")

    logger.info(s"Inizio Scrittura ingestion flussi di misura Int su tabella ${erpValidatedIntDao.tableName}")
    erpValidatedIntDao.write(erpValidationIntDf, true)
    logger.info(s"Fine Scrittura ingestion flussi di misura Int su tabella ${erpValidatedIntDao.tableName}")


    // calcolo dei contributi immesso e prelevato
    val erpValidationIntReadedDfAll = erpValidatedIntDao.read()
    // Use only the current run's validated data to avoid mixing histories
    val erpValidationIntReadedDf = erpValidationIntReadedDfAll
      .filter(col(erpValidatedIntSchema.executionid) === lit(timestamp))

    logger.info("Inizio calcolo dei contributi immesso e prelevato")
    val erpDettaglioInt = CalcoloPrelevatoPuntiPrelievoOrariINTTrasformation.calcoloErpDettaglioInt(
      erpValidationIntReadedDf,
      rcuAziendaP,
      args,
      singola_piva_distributore
    )
    logger.info("Fine calcolo dei contributi immesso e prelevato")

    logger.info(s"Inizio Scrittura calcolo dei contributi immesso e prelevato su tabella ${erpDettaglioIntDao.tableName}")
    erpDettaglioIntDao.write(erpDettaglioInt, true)
    logger.info(s"Fine Scrittura calcolo dei contributi immesso e prelevato su tabella ${erpDettaglioIntDao.tableName}")

    val erpDettaglioReadedDfAll = erpDettaglioIntDao.read()
    // Aggregate only the current run's detail rows
    val erpDettaglioReadedDf = erpDettaglioReadedDfAll
      .filter(col(erpDettaglioINTSchema.executionid) === lit(timestamp))

    logger.info("Inizio aggregazione giornaliera punti di interconnesione")
    val erpAggregatoIntDf = CalcoloPrelevatoPuntiPrelievoOrariINTTrasformation
      .calcoloErpAggregatoInt(erpDettaglioReadedDf, timestamp)
    logger.info("Fine aggregazione giornaliera punti di interconnesione")

    logger.info(s"Inizio Scrittura aggregzione giornaliera punti di interconnesione su tabella ${erpAggregatoIntDao.tableName}")
    erpAggregatoIntDao.write(erpAggregatoIntDf, true)
    logger.info(s"Fine Scrittura aggregzione giornaliera punti di interconnesione su tabella ${erpAggregatoIntDao.tableName}")

    logger.info("Fine Calcolo Saldo Energetico ai Punti di Interconnesione")
  }

}
