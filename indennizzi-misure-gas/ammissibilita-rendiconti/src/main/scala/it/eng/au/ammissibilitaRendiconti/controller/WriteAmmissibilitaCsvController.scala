package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.model.{ReportAmmissibilitaRzg1, ZipRzg1Metadata}
import it.eng.au.ammissibilitaRendiconti.schema.AmmissibilitaCsvSchema._
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants.{AMMISSIBILITA_NO_0, AMMISSIBILITA_SI_1, CSV_DELIMITER}
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import it.eng.au.ammissibilitaRendiconti.utility.file.FileUtility
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/** Si occupa di scrivere il file di ammissibilità relativo al file ZIP RZG1 trasmesso. */
object WriteAmmissibilitaCsvController extends Serializable {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  //  PIVA_UTENTE SI
  //  VERIFICA_AMM SI 0 = Negativo, 1 = Positivo
  //  COD_CAUSALE SI (se VERIFICA_AMM = 0)
  //  MOTIVAZIONE SI (se VERIFICA_AMM = 0)
  def writeCsv(zipMetadata: RDD[ZipRzg1Metadata]): RDD[ReportAmmissibilitaRzg1] = {
    val daterun = Environment.startDateTime
    val year = Properties.getCurrentYear
    val month = Properties.getCurrentMonth
    val basePath = Properties.getRzg1AmmOutputPath
    val header = List(PIVA_UTENTE, VERIFICA_AMM, COD_CAUSALE, MOTIVAZIONE).mkString(CSV_DELIMITER)
    zipMetadata.map(zipRzg1 => {
      val pivaUtente = zipRzg1.pivaUtente
      val verificaAmm = if (zipRzg1.isAmmissibile) AMMISSIBILITA_SI_1 else AMMISSIBILITA_NO_0
      val codCasuale = zipRzg1.statusCode
      val motivazione = zipRzg1.statusMessage
      val record = List(pivaUtente, verificaAmm, codCasuale, motivazione).mkString(CSV_DELIMITER)

      val (middlePath, csvName) = getOutputFolderCsvName(pivaUtente, zipRzg1.file.getName, daterun, year, month)
      val outputFolderPath = basePath + middlePath
      val fullPath = outputFolderPath + csvName

      val outputFolder = new File(outputFolderPath)

      val ammissibilitaRzg = ReportAmmissibilitaRzg1(
        cartella_cloud = zipRzg1.file.getParent,
        zip_file_name = zipRzg1.file.getName,
        zip_last_modified_date = zipRzg1.lastModified,
        csv_file_name = zipRzg1.csv.map(_.fileName).getOrElse(""),
        cartella_cloud_ammissibilita = outputFolderPath,
        ammissibilita_file_name = csvName,
        ammissibilita = zipRzg1.isAmmissibile,
        codice = zipRzg1.statusCode,
        descrizione = zipRzg1.statusMessage,
        data_creazione = Timestamp.valueOf(daterun),
        annomese = zipRzg1.annoMeseCompetenza
      )
      if (outputFolder.exists() && outputFolder.canWrite) {
        FileUtility.writeCsv(fullPath, header, List(record), isTmpFolder = false, appendMode = true)
        ammissibilitaRzg
      }
      else {
        logger.warn(s"Couldn't write to ${ammissibilitaRzg.cartella_cloud_ammissibilita}, the path doesn't exist or it's not writable")
        ammissibilitaRzg.copy(cartella_cloud_ammissibilita = s"Couldn't write to ${ammissibilitaRzg.cartella_cloud_ammissibilita}, the path doesn't exist or it's not writable")
      }
    })
  }

  //<PIVA_UdD>_<PIVA_Id>_AAAAMM_RZG2_Timestamp_progressivo.zip
  //<Nome_file_RZG1>_AMM_<timestamp>.csv
  //Nome_file_RZG1 = Nome del file su cui si sta fornendo l’ammissibilità;
  //CIG2_PIVAUDD/AAAA/MM/
  def getOutputFolderCsvName(piva: String, inputFileName: String, daterun: LocalDateTime, year: String, month: String): (String, String) = {
    val timestamp = daterun.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))

    val inputFileNameWithoutExtension = inputFileName.replace(".zip", "")

    (s"/CIG1_$piva/$year/$month", s"/${inputFileNameWithoutExtension}_AMM_${timestamp}.csv")
  }


}
