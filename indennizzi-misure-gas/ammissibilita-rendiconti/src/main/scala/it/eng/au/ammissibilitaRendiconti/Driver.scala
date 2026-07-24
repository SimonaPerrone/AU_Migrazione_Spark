package it.eng.au.ammissibilitaRendiconti

import it.eng.au.ammissibilitaRendiconti.args.RendicontiArgsFactory
import it.eng.au.ammissibilitaRendiconti.controller.MailLogController.mailLogger
import it.eng.au.ammissibilitaRendiconti.controller.ReadCsvController._
import it.eng.au.ammissibilitaRendiconti.controller.ValidateIndennizziController._
import it.eng.au.ammissibilitaRendiconti.controller._
import it.eng.au.ammissibilitaRendiconti.dao.{AggregatoTotaleDAO, DeltaEuroDAO, IndennizziRzg2DAO, ReportAmmissibilitaRzg1DAO}
import it.eng.au.ammissibilitaRendiconti.model.rules.{CsvRule, IndennizziRule, ZipRule}
import it.eng.au.ammissibilitaRendiconti.utility.environment.{AmmissibilitaEnvironment, Properties}
import it.eng.au.indennizziMisureGasCommon.utility.log.LogUtility
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = RendicontiArgsFactory.parse(args)

      AmmissibilitaEnvironment.setEnvironment(parsedArgs)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()
    } catch {
      case e: Throwable =>
        logger.error(s"An error occurred in the procedure.")
        throw e
    }
  }

  def run(): Unit = {
    mailLogger.warn(s"Procedura di ammissibilita' ${LocalDate.now().toString} in esecuzione...")
    val reportAmmissibilitaDAO = new ReportAmmissibilitaRzg1DAO()
    val aggregatoTotaleDAO = new AggregatoTotaleDAO()
    val reportAmmissibilitaRendicontiDAO = new ReportAmmissibilitaRzg1DAO()
    val indennizziRzg2DAO = new IndennizziRzg2DAO()
    val deltaEuroDAO = new DeltaEuroDAO()

    /** Regole di ammissibilità per i file ZIP. */
    val zipRules: List[ZipRule] = List(
      ruleCheckZipIntegrity, //3
      ruleCheckZipBody, //4
      ruleCheckZipAndCsvNames, //5
      ruleCheckPiva, //6
      ruleCheckCsvEncoding) //7

    /** Regole di ammissibilità per i file CSV. */
    val csvRules: List[CsvRule] = List(
      ruleCheckHeader, //10
      ruleCheckDate, //11
      ruleCheckIdIndennizzo, //12
      ruleCheckPivaId, //13
      ruleCheckPivaUdd, //14
      ruleCheckRagSocId, //15
      ruleCheckRagSocUdd, //16
      ruleCheckPivaIdConsistency, //21
      ruleCheckPivaUddConsistency //22
    )

    /** Regole di ammissibilità per i campi relativi agli indennizzi. */
    //l'ordine è importante quando si richiama questi 2 metodi perché se aggregatoTotale è vuoto deve prevalere il codice di non ammissibilità dell'indennizzo (COD_011)
    val indennizziRules: List[IndennizziRule] = List(
      ruleValidateOM1, //17
      ruleValidateOM2, //18
      ruleValidateOM3, //19
      ruleCheckAtLeastOneOMIsValued, //23
      ruleValidateIdIndennizzo //20
    )

    val isRuleCheckNumberOfCsvFieldsEnabled = Properties.isRuleCheckNumberOfCsvFieldsEnabled

    /** Lettura dei file ZIP */
    val (zipFileWithMeta, yearMonthMin, yearMonthMax) = ReadZipController.readZip()
    mailLogger.warn("1. Lettura degli zip RZG1 -> ok")

    /** Tabella cig_aggregato_totale (utilizzata successivamente per uno dei controlli di ammissibilità) */
    val aggregatoTotale = aggregatoTotaleDAO.get
    /** Tabella cig_report_ammissibilita, utilizzata per i controlli alreadyTransmitted e alreadyCompued. */
    val reportAmmissibilitaRzg1 = reportAmmissibilitaDAO.getReportAmmissibilita(yearMonthMin, yearMonthMax)
    /** RDD degli ZIP già processati. */
    val alreadyComputedZips = reportAmmissibilitaDAO.getAlreadyComputedZips(reportAmmissibilitaRzg1)
    /** RDD degli ZIP già trasmessi. */
    val alreadyTransmittedZips = reportAmmissibilitaDAO.getAlreadyTransmittedZips(reportAmmissibilitaRzg1)

    /** ZIP da lavorare in questa sessione (ovvero ZIP letti che non sono stati già processati) */
    val notYetComputedZips = AlreadyComputedZipsController.filterAlreadyComputedZips(zipFileWithMeta, alreadyComputedZips)
      .persist(StorageLevel.MEMORY_AND_DISK)
    /** ZIP ammissibili da processare. */
    val correctZipFileWithMeta = notYetComputedZips.filter(_.isAmmissibile)
    /** ZIP non ammissibili. */
    val incorrectZipFileWithMeta = notYetComputedZips.filter(!_.isAmmissibile)
    mailLogger.warn("2. Rimozione degli zip gia' processati -> ok")

    /** ZIP da lavorare in aggiunta alle info sul controllo alreadyTransmitted. In altre parole, i ZIP già trasmessi sono stati contrassegnati e finiranno tra gli ZIP non ammissibili. */
    val zipFileWithAlreadyTransmittedInfo = AlreadyTransmittedRzg1FileController.getAlreadyTransmitted(correctZipFileWithMeta, alreadyTransmittedZips)
      .persist(StorageLevel.MEMORY_AND_DISK)
    /** ZIP ammissibili da processare, ovvero non già trasmessi. */
    val notYetTransmittedZip = zipFileWithAlreadyTransmittedInfo.filter(_.isAmmissibile)
    /** ZIP già trasmessi tra quelli che abbiamo letto */
    val alreadyTransmittedZip = zipFileWithAlreadyTransmittedInfo.filter(!_.isAmmissibile)
    mailLogger.warn("3. Rimozione degli zip gia' trasmessi -> ok")

    /** ZIP con le informazioni sul CSV interno e sull'ammissibilità di quest'ultimo. */
    val unzipCsv = notYetTransmittedZip.map(ReadCsvController.checkAndUnzip(_, zipRules, csvRules, isRuleCheckNumberOfCsvFieldsEnabled))
      .persist(StorageLevel.MEMORY_AND_DISK)
    /** ZIP decompressi e ammissibili. */
    val correctUnzipCsv = unzipCsv.filter(_.isAmmissibile)
    /** ZIP decompressi ma inammissibili (poiché è fallita qualche regola di ammissibilità sul CSV). */
    val incorrectUnzipCsv = unzipCsv.filter(!_.isAmmissibile)
    mailLogger.warn("4. Unzip dei file CSV e ammissibilita' degli stessi -> ok")

    val joinZipIndennizzi = JoinController.joinRzg1Indennizzi(correctUnzipCsv, aggregatoTotale)
    /** ZIP decompressi di cui è stato effettuato anche il controllo sugli indennizzi nel CSV. */
    val indennizziValidated = joinZipIndennizzi.map({ case (zipMeta, aggregatoTotale) => ValidateIndennizziController.validate(zipMeta, aggregatoTotale, indennizziRules) }).persist(StorageLevel.MEMORY_AND_DISK)
    /** ZIP decompressi e ammissibili. */
    val correctIndennizzi = indennizziValidated.filter({ case (zipMetadata, _) => zipMetadata.isAmmissibile })
    mailLogger.warn("5. Validazione degli indennizzi -> ok")
    /** Dataframe degli indennizzi da scrivere nella cig_indennizzi_rzg2. */
    val indennizziRzg2 = indennizziRzg2DAO.get(correctIndennizzi)
      .persist(StorageLevel.MEMORY_AND_DISK)
    indennizziRzg2DAO.write(indennizziRzg2)
    mailLogger.warn("6. Scrittura della tabella cig_indennizzi_rzg2 -> ok")

    /** Dataframe delle differenze tra gli indennizzi inviati e ricevuti, cig_delta_euro. */
    val deltaEuro = deltaEuroDAO.get(indennizziRzg2)
    deltaEuroDAO.write(deltaEuro)
    mailLogger.warn("7. Scrittura della tabella cig_delta_euro -> ok")

    /** Union di tutti gli RDD creati finora (ammissibili e non ammissibili) per la scrittura delle ammissibilità di ognuno degli RZG1 processati. */
    val indennizziUnionForAmmissibilita = indennizziValidated
      .map({ case (zipMetadata, _) => zipMetadata })
      .union(alreadyTransmittedZip).coalesce(indennizziValidated.getNumPartitions)
      .union(incorrectUnzipCsv).coalesce(indennizziValidated.getNumPartitions)
      .union(incorrectZipFileWithMeta).coalesce(indennizziValidated.getNumPartitions)

    /** Informazioni sull'avvenuta scrittura dei file di ammissibilità (o meno). */
    val ammissibilita = WriteAmmissibilitaCsvController.writeCsv(indennizziUnionForAmmissibilita)
    mailLogger.warn("8. Scrittura dei file di ammissibilita' RZG1_AMM -> ok")
    /** Dataframe di reportistica dd scrivere nella cig_report_ammissibilita_rzg1. */
    val reportAmmissibilitaRzg1Output = reportAmmissibilitaRendicontiDAO.getReportAmmissibilitaOutput(ammissibilita)
      .persist(StorageLevel.MEMORY_AND_DISK)
    reportAmmissibilitaRendicontiDAO.writeTable(reportAmmissibilitaRzg1Output)
    mailLogger.warn("9. Scrittura della tabella cig_report_ammissibilita_rzg1 -> ok")

    /** Check dei vari controlli di correttezza del processo per l'ARU-12. */
    MailLogController.check(notYetComputedZips, reportAmmissibilitaRendicontiDAO, yearMonthMin, yearMonthMax)
  }
}