package it.eng.au.ammissibilitaSettlementGas

import it.eng.au.ammissibilitaSettlementGas.args.ArgsFactory
import it.eng.au.ammissibilitaSettlementGas.controller.QKRIUDController._
import it.eng.au.ammissibilitaSettlementGas.controller.TFCController._
import it.eng.au.ammissibilitaSettlementGas.controller.VPGController._
import it.eng.au.ammissibilitaSettlementGas.controller._
import it.eng.au.ammissibilitaSettlementGas.dao._
import it.eng.au.ammissibilitaSettlementGas.model.rules._
import it.eng.au.ammissibilitaSettlementGas.schema.{PubblicazioneAmmissibilitaQKRIUDSchema, PubblicazioneAmmissibilitaTFCSchema, PubblicazioneAmmissibilitaVPGSchema}
import it.eng.au.ammissibilitaSettlementGas.utility.environment.AmmissibilitaEnvironment
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import it.eng.au.trasmissioneSettlementGasCommon.utility.log.LogUtility
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = ArgsFactory.parse(args)

      AmmissibilitaEnvironment.setEnvironment(parsedArgs)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()

    } catch {
      case e: Exception => logger.error(e.getStackTrace); throw e
      case e: Error => logger.error(e.getStackTrace); throw e
    }
  }

  def run(): Unit = {
    val TFCFileDAO = new TFCFileDao()
    val VPGFileDAO = new VPGFileDao()
    val QKRIUDFileDao = new QKRIUDFileDao
    val ReportAmmissibilitaTFCDao = new ReportAmmissibilitaTFCDao()
    val ReportAmmissibilitaVPGDao = new ReportAmmissibilitaVPGDao()
    val reportAmmissibilitaQKRIUDDao = new ReportAmmissibilitaQKRIUDDao
    val PubblicazioneAmmissibilitaTFCDao = new PubblicazioneAmmissibilitaTFCDao()
    val PubblicazioneAmmissibilitaVPGDao = new PubblicazioneAmmissibilitaVPGDao()
    val pubblicazioneAmmissibilitaQKRIUDDao = new PubblicazioneAmmissibilitaQKRIUDDao
    val TSGTFCDao = new TSGTFCDao()
    val TSGVPGDao = new TSGVPGDao()
    val TSGQKRIUDDao = new TSGQKRIUDDao
    val rcugasConnessioniDistr2RemiDao = new CodRemiAnagraficaDao

    val rcugasConnessioniDistr2RemiSet = Environment.spark.sparkContext.broadcast(
      rcugasConnessioniDistr2RemiDao.readTable.collect.toSet)

    val TFCCsvRules: List[TFCCsvRule] = List(
      ruleCheckTFCPivaRdb, // 3
      ruleCheckTFCFileExtension, // 4
      TFCController.ruleCheckTFCFileIntegrity,
      ruleCheckTFCFileHeader // 5
      //ruleCheckTFCAlreadyTransmitted // 6
    )

    val TFCRecordRules: List[TFCRecordRule] = List(
      ruleCheckTFCDate, // 6
      ruleCheckTFCDateFormat, // 7 & 8
      ruleCheckTFCDateConsistence, // 9
      ruleCheckTFCRegClimMandatory, // 10
      ruleCheckTFCRegClimMismatch, // 11
      ruleCheckTFCWKRDefined, // 12
      ruleCheckTFCWKRFormat, // 13
      ruleCheckTFCFieldsNumber // 14
    )

    // TODO: For reading Csv files, Tfc and vpg ones as well, we have to select files sent
    //  the day before "today". Cosa utilizzo per far ciò? Devo forse utilizzare lastModified???


    val readTFCFolderResult = TFCController.readCsvFiles()
    val TFCFilesWithMeta = readTFCFolderResult._1
    val yearMonthMin = readTFCFolderResult._2
    val yearMonthMax = readTFCFolderResult._3
    //val (TFCFilesWithMeta, yearMonthMin, yearMonthMax) = TFCController.readCsvFiles() // FIXME: Vorrei farlo così ma non funge.

    val pubbAmmTFC = PubblicazioneAmmissibilitaTFCDao.getPubblicazioneAmmissibilitaTFC(yearMonthMin, yearMonthMax)
    val alreadyComputedTFCFiles = PubblicazioneAmmissibilitaTFCDao.getAlreadyComputedTFCFiles(pubbAmmTFC)
    val notYetComputedTFCFiles = AlreadyComputedTFCController.filterAlreadyComputedTFCs(TFCFilesWithMeta, alreadyComputedTFCFiles)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val correctTFCFilesWithMeta = notYetComputedTFCFiles.filter(_.isAmmissibile)
    val incorrectTFCFilesWithMeta = notYetComputedTFCFiles.filter(!_.isAmmissibile)

    val alreadyTransmittedTFC = PubblicazioneAmmissibilitaTFCDao.getAlreadyTransmittedTFC(pubbAmmTFC)
    val TFCFileWithAlreadyTransmittedInfo = AlreadyTransmittedTFCController.getAlreadyTransmitted(correctTFCFilesWithMeta, alreadyTransmittedTFC)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val notYetTransmittedTFCFiles = TFCFileWithAlreadyTransmittedInfo.filter(_.isAmmissibile)
    val alreadyTransmittedTFCFiles = TFCFileWithAlreadyTransmittedInfo.filter(!_.isAmmissibile)

    val checkedTFCFiles = notYetTransmittedTFCFiles.map(tfcMeta => TFCController.checkTFCCsv(tfcMeta, TFCCsvRules))
      .union(incorrectTFCFilesWithMeta).coalesce(notYetTransmittedTFCFiles.getNumPartitions)
      .union(alreadyTransmittedTFCFiles).coalesce(notYetTransmittedTFCFiles.getNumPartitions)

    // Ora dovrei popolare le tabelle TSG2_TFC_FILE e TSG2_REPORT_AMMISSIBILITA_TFC

    // Popolamento tabella TSG2_TFC_FILE
    val dfTFCFile = TFCFileDAO.get(notYetComputedTFCFiles)
    TFCFileDAO.write(dfTFCFile)

    // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_TFC

    val TFCFilesAndRecords = checkedTFCFiles.map(tfcMeta =>
      if (tfcMeta.isAmmissibile) tfcMeta.copy(csv = getTFCListFromFile(tfcMeta))
      else tfcMeta)

    val TFCFilesAndRecordsChecked = TFCFilesAndRecords.
      map(tfcMeta => tfcMeta.copy(csv = Some(tfcMeta.csv.getOrElse(List()).map(tfcRec => TFCController.checkTFCRecord(tfcRec, TFCRecordRules)))))

    val reportPubblicazioneAmmissibilitaTFC = TFCController.writeCSVTFCAmmFile(TFCFilesAndRecordsChecked)
    val dfReportPubblicazioneAmmTFC = Environment.spark.createDataFrame(reportPubblicazioneAmmissibilitaTFC)
      .toDF(PubblicazioneAmmissibilitaTFCSchema.CARTELLA_CLOUD, PubblicazioneAmmissibilitaTFCSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaTFCSchema.LAST_MODIFIED, PubblicazioneAmmissibilitaTFCSchema.AMMISSIBILITA_FILE_NAME)
      .dropDuplicates()

    val dfReportAmmissibilitaTFC = ReportAmmissibilitaTFCDao.get(TFCFilesAndRecordsChecked)
      .persist(StorageLevel.MEMORY_AND_DISK)
    ReportAmmissibilitaTFCDao.write(dfReportAmmissibilitaTFC)

    val dfPubblAmmTFC = PubblicazioneAmmissibilitaTFCDao.get(dfReportAmmissibilitaTFC, dfReportPubblicazioneAmmTFC)
    PubblicazioneAmmissibilitaTFCDao.write(dfPubblAmmTFC)

    val dfTSGTFC = TSGTFCDao.get(dfReportAmmissibilitaTFC)
    TSGTFCDao.write(dfTSGTFC)

    val VPGCsvRules: List[VPGCsvRule] = List(
      ruleCheckVPGPivaRdb, // 3
      ruleCheckVPGFileExtension, // 4
      VPGController.ruleCheckVPGFileIntegrity,
      ruleCheckVPGFileHeader // 5
    )

    val VPGRecordRules: List[VPGRecordRule] = List(
      ruleCheckVPGDate, // 6 & 7
      ruleCheckVPGDateConsistence, // 8
      ruleCheckVPGMandatoryFields, // 9
      ruleCheckVPGFieldsFormat, // 10
      ruleCheckVPGFieldsNumber // 11
    )

    val VPGFilesWithMeta = VPGController.readCsvFiles()

    val pubbAmmVPG = PubblicazioneAmmissibilitaVPGDao.getPubblicazioneAmmissibilitaVPG()
    val alreadyComputedVPGFiles = PubblicazioneAmmissibilitaVPGDao.getAlreadyComputedVPGFiles(pubbAmmVPG)
    val notYetComputedVPGFiles = AlreadyComputedVPGController.filterAlreadyComputedVPGs(VPGFilesWithMeta, alreadyComputedVPGFiles)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val correctVPGFilesWithMeta = notYetComputedVPGFiles.filter(_.isAmmissibile)
    val incorrectVPGFilesWithMeta = notYetComputedVPGFiles.filter(!_.isAmmissibile)

    val alreadyTransmittedVPG = PubblicazioneAmmissibilitaVPGDao.getAlreadyTransmittedVPG(pubbAmmVPG)
    val VPGFileWithAlreadyTransmittedInfo = AlreadyTransmittedVPGController.getAlreadyTransmitted(correctVPGFilesWithMeta, alreadyTransmittedVPG)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val notYetTransmittedVPGFiles  = VPGFileWithAlreadyTransmittedInfo.filter(_.isAmmissibile)
    val alreadyTransmittedVPGFiles = VPGFileWithAlreadyTransmittedInfo.filter(!_.isAmmissibile)

    val checkedVPGFiles = notYetTransmittedVPGFiles.map(vpgMeta => VPGController.checkVPGCsv(vpgMeta, VPGCsvRules))
      .union(incorrectVPGFilesWithMeta).coalesce(notYetTransmittedVPGFiles.getNumPartitions)
      .union(alreadyTransmittedVPGFiles).coalesce(notYetTransmittedVPGFiles.getNumPartitions)

    // Popolamento tabella TSG2_VPG_FILE
    val dfVPGFile = VPGFileDAO.get(notYetComputedVPGFiles)
    VPGFileDAO.write(dfVPGFile)

    // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_VPG
    val VPGFilesAndRecords = checkedVPGFiles.map(vpgMeta =>
      if (vpgMeta.isAmmissibile) vpgMeta.copy(csv = getVPGListFromFile(vpgMeta.file))
      else vpgMeta)

    val VPGFilesAndRecordsChecked = VPGFilesAndRecords
      .map(vpgMeta => vpgMeta.copy(csv = Some(vpgMeta.csv.getOrElse(List())
        .map(vpgRec => VPGController.checkVPGRecord(vpgRec, VPGRecordRules)))))

    val dfReportAmmissibilitaVPG = ReportAmmissibilitaVPGDao.get(VPGFilesAndRecordsChecked)
      .persist(StorageLevel.MEMORY_AND_DISK)
    ReportAmmissibilitaVPGDao.write(dfReportAmmissibilitaVPG)

    val reportPubblicazioneAmmissibilitaVPG = VPGController.writeCSVVPGAmmFile(VPGFilesAndRecordsChecked)
    val dfReportPubblicazioneAmmVPG = Environment.spark.createDataFrame(reportPubblicazioneAmmissibilitaVPG)
      .toDF(PubblicazioneAmmissibilitaVPGSchema.CARTELLA_CLOUD, PubblicazioneAmmissibilitaVPGSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaVPGSchema.LAST_MODIFIED, PubblicazioneAmmissibilitaVPGSchema.AMMISSIBILITA_FILE_NAME)
      .dropDuplicates()

    val dfPubblAmmVPG = PubblicazioneAmmissibilitaVPGDao.get(dfReportAmmissibilitaVPG, dfReportPubblicazioneAmmVPG)
    PubblicazioneAmmissibilitaVPGDao.write(dfPubblAmmVPG)

    val dfTSGVPG = TSGVPGDao.get(dfReportAmmissibilitaVPG)
    TSGVPGDao.write(dfTSGVPG)

    val QKRIUDCsvRules: List[QKRIUDCsvRule] = List(
      ruleCheckQKRIUDPivaRdb, // 2
      ruleCheckQKRIUDFileExtension, //3
      ruleCheckQKRIUDFileIntegrity, //4
      ruleCheckQKRIUDFileHeader //6
    )

    val QKRIUDRecordRules: List[QKRIUDRecordRule] = List(
      ruleCheckQKRIUDDate, //7
      ruleCheckQKRIUDDateConsistence, //8
      ruleCheckQKRIUDCodRemiMandatory, //9
      ruleCheckQKRIUDfieldMandatory, //10
      ruleCheckQKRIUDDateFormat, //11
      ruleCheckQKRIUDDateExistance, //12
      ruleCheckQKRIUDCodRemiFormat, //13
      ruleCheckQKRIUDfieldFormat,  //14
      ruleCheckCodRemiExistanceRcugas(rcugasConnessioniDistr2RemiSet) //15
    )

    val QKRIUDFilesWithMeta = QKRIUDController.readCsvFiles()

    val pubbAmmQKRIUD = pubblicazioneAmmissibilitaQKRIUDDao.getPubblicazioneAmmissibilitaQKRIUD
    val alreadyComputedQKRIUDFiles = pubblicazioneAmmissibilitaQKRIUDDao.getAlreadyComputedQKRIUDFiles(pubbAmmQKRIUD)
    val notYetComputedQKRIUDFiles = AlreadyComputedQKRIUDController.filterAlreadyComputedQKRIUDs(QKRIUDFilesWithMeta, alreadyComputedQKRIUDFiles)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val correctQKRIUDFilesWithMeta = notYetComputedQKRIUDFiles.filter(_.isAmmissibile)
    val incorrectQKRIUDFilesWithMeta = notYetComputedQKRIUDFiles.filter(!_.isAmmissibile)

    val alreadyTransmittedQKRIUD = pubblicazioneAmmissibilitaQKRIUDDao.getAlreadyTransmittedQKRIUD(pubbAmmQKRIUD)
    val QKRIUDFileWithAlreadyTransmittedInfo = AlreadyTransmittedQKRIUDController.getAlreadyTransmitted(correctQKRIUDFilesWithMeta, alreadyTransmittedQKRIUD)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val notYetTransmittedQKRIUDFiles = QKRIUDFileWithAlreadyTransmittedInfo.filter(_.isAmmissibile)
    val alreadyTransmittedQKRIUDFiles = QKRIUDFileWithAlreadyTransmittedInfo.filter(!_.isAmmissibile)

    val checkedQKRIUDFiles = notYetTransmittedQKRIUDFiles.map(qkriudMeta => QKRIUDController.checkQKRIUDCsv(qkriudMeta, QKRIUDCsvRules))
      .union(incorrectQKRIUDFilesWithMeta).coalesce(notYetTransmittedQKRIUDFiles.getNumPartitions)
      .union(alreadyTransmittedQKRIUDFiles).coalesce(notYetTransmittedQKRIUDFiles.getNumPartitions)

    // Popolamento tabella TSG2_QKRIUD_FILE
    val dfQKRIUDFile = QKRIUDFileDao.get(notYetComputedQKRIUDFiles)
    QKRIUDFileDao.write(dfQKRIUDFile)

    // Popolamento tabella TSG2_REPORT_AMMISSIBILITA_QKRIUD
    val QKRIUDFilesAndRecords = checkedQKRIUDFiles.map(qkriudMeta =>
      if (qkriudMeta.isAmmissibile) qkriudMeta.copy(csv = getQKRIUDListFromFile(qkriudMeta.file))
      else qkriudMeta)

    val QKRIUDFilesAndRecordsChecked = QKRIUDFilesAndRecords
      .map(qkriudMeta => qkriudMeta.copy(csv = Some(qkriudMeta.csv.getOrElse(List())
        .map(QKRIUDRec => QKRIUDController.checkQKRIUDRecord(QKRIUDRec, QKRIUDRecordRules)))))

    val dfReportAmmissibilitaQKRIUD = reportAmmissibilitaQKRIUDDao.get(QKRIUDFilesAndRecordsChecked)
      .persist(StorageLevel.MEMORY_AND_DISK)
    reportAmmissibilitaQKRIUDDao.write(dfReportAmmissibilitaQKRIUD)

    val reportPubblicazioneAmmissibilitaQKRIUD = QKRIUDController.writeCSVQKRIUDAmmFile(QKRIUDFilesAndRecordsChecked)
    val dfReportPubblicazioneAmmQKRIUD = Environment.spark.createDataFrame(reportPubblicazioneAmmissibilitaQKRIUD)
      .toDF(PubblicazioneAmmissibilitaQKRIUDSchema.CARTELLA_CLOUD, PubblicazioneAmmissibilitaQKRIUDSchema.CSV_FILE_NAME,
        PubblicazioneAmmissibilitaQKRIUDSchema.DATA_AMM, PubblicazioneAmmissibilitaQKRIUDSchema.AMMISSIBILITA_FILE_NAME)
      .dropDuplicates()

    val dfPubblAmmQKRIUD = pubblicazioneAmmissibilitaQKRIUDDao.get(dfReportAmmissibilitaQKRIUD, dfReportPubblicazioneAmmQKRIUD)
    pubblicazioneAmmissibilitaQKRIUDDao.write(dfPubblAmmQKRIUD)

    val dfTSGQKRIUD = TSGQKRIUDDao.get(dfReportAmmissibilitaQKRIUD)
    TSGQKRIUDDao.write(dfTSGQKRIUD)
  }
}
