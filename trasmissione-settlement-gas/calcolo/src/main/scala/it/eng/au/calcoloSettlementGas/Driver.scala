package it.eng.au.calcoloSettlementGas

import it.eng.au.calcoloSettlementGas.args.ArgsFactory
import it.eng.au.calcoloSettlementGas.controller.CalcoloController
import it.eng.au.calcoloSettlementGas.dao.{AtgTabProfiliGiornStdPercBkpDao, AtgTabProfiliGiornStdPercDao, CodRemiAnagraficaDao, TSGQKRIUDDao, TSGTFCDao, TSGVPGDao, TabParametriCaratteristiciProfPrelDao, TabProfiliGiornStdPercDao}
import it.eng.au.calcoloSettlementGas.schema.{AtgTabProfiliGiornStdPercBkpSchema, AtgTabProfiliGiornStdPercSchema}
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.calcoloSettlementGas.utility.environment.CalcoloEnvironment
import it.eng.au.trasmissioneSettlementGasCommon.schema.{TSGQKRIUDSchema, TSGTFCSchema, TSGVPGSchema}
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import it.eng.au.trasmissioneSettlementGasCommon.utility.log.LogUtility
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.storage.StorageLevel

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = ArgsFactory.parse(args)

      CalcoloEnvironment.setEnvironment(parsedArgs)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()

    } catch {
      case e: Exception => logger.error(e.getStackTrace); throw e
      case e: Error => logger.error(e.getStackTrace); throw e
    }
  }

  def run(): Unit = {
    val TSGVPGDao = new TSGVPGDao()
    val TSGTFCDao = new TSGTFCDao()
    val TSGQKRIUDDao = new TSGQKRIUDDao()
    val TabParamCarattProfPrelDao = new TabParametriCaratteristiciProfPrelDao()
    val TabProfiliGiornStdPercDao = new TabProfiliGiornStdPercDao()
    val AtgTabProfiliGiornStdPercDao = new AtgTabProfiliGiornStdPercDao()
    val AtgTabProfiliGiornStdPercBkpDao = new AtgTabProfiliGiornStdPercBkpDao()
    val codRemiAnagraficaDao = new CodRemiAnagraficaDao

    val annoMese = Properties.getAnnoMese
    val numberOfDays = Properties.getDaysInMonth

    val tsgVpg = TSGVPGDao.readTablePartiton(annoMese)
    val tsgTfc = TSGTFCDao.readTablePartiton(annoMese)
    val tsgQkriud = TSGQKRIUDDao.readTablePartiton(annoMese).filter(col(TSGQKRIUDSchema.verifica_amm) === true)
    val remiAnagrafica = codRemiAnagraficaDao.readTable

    val vpgLatestRecords = CalcoloController
      .getLatestVPGRecords(tsgVpg.filter(col(TSGVPGSchema.verifica_amm) === true))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val tfcLatestRecords = CalcoloController
      .getLatestTFCRecords(tsgTfc.filter(col(TSGTFCSchema.verifica_amm) === true))
      .persist(StorageLevel.MEMORY_AND_DISK)
    val qkriudLatestRecords = CalcoloController
      .getLatestQKRIUDRecords(tsgQkriud)
      .persist(StorageLevel.MEMORY_AND_DISK)


    if (vpgLatestRecords.count != numberOfDays.toLong) {
      logger.error(s"Il numero dei record presenti nella tabella VPG per l'anno-mese $annoMese è ${vpgLatestRecords.count}, e dovrebbe essere $numberOfDays")
      throw new Exception(s"Il numero dei record presenti nella tabella VPG per l'anno-mese $annoMese è ${vpgLatestRecords.count}, e dovrebbe essere $numberOfDays")
    }

    val dfTabParamCarattProfPrel = TabParamCarattProfPrelDao.readTable // AGGIUNTO ORA


    val dfTabProfiliGiornStdPerc = CalcoloController.calcoloPprof(tfcLatestRecords, vpgLatestRecords, qkriudLatestRecords, dfTabParamCarattProfPrel, remiAnagrafica)


    TabProfiliGiornStdPercDao.write(dfTabProfiliGiornStdPerc)

    // TODO overwrite della tabella finale (se si partiziona per annomese è meglio, così sovrascriviamo l'itera partizione)

    // 1) Leggere annomese di competenza (la partizione relativa) dalla atg e scrivere tale partizione sulla tabella di bkp. (scrivere anche execId)
    val dfAtgTabProfiliGiornStdPerc = AtgTabProfiliGiornStdPercDao.readTablePartiton(annoMese)
    AtgTabProfiliGiornStdPercBkpDao.write(dfAtgTabProfiliGiornStdPerc.withColumn(AtgTabProfiliGiornStdPercBkpSchema.executionid, lit(Environment.executionId).cast(LongType)))

    // 2) dfTabProfiliGiornStdPerc sovrascrivere sulla atg facendo prima ovviamente una selectExpr opportuna partizionando per annomese
    AtgTabProfiliGiornStdPercDao.write(dfTabProfiliGiornStdPerc.withColumn(AtgTabProfiliGiornStdPercSchema.annomese, lit(annoMese))
      .selectExpr(AtgTabProfiliGiornStdPercSchema.getValues: _*))
  }

}
