package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.model.ZipRzg1Metadata
import it.eng.au.ammissibilitaRendiconti.schema.CsvRecoverySchema
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants.{CIG1, COD_904, MOTIVAZIONE_NOME_ZIP, YEARMONTH_MAX, YEARMONTH_MIN}
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File
import scala.util.matching.Regex

object ReadZipController extends Serializable {
  //<PIVA_Id>_<PIVA_UdD>_AAAAMM_RZG1_Timestamp_progressivo.zip
  private val zipFileRegex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d{6})_(RZG1)_(\\d+)_(\\d+)\\.(?i)zip".r

  /**
   * Se la modalità recovery non è attiva, legge i file ZIP relativi a un certo anno-mese ([[Properties.getCurrentYear]], [[Properties.getCurrentMonth]]);
   * altrimenti legge i file ZIP indicati nel file CSV di recovery. Successivamente, ne estrae i metadati e ne effettua l'ammissibilità.
   * @return un RDD contenente i metadati degli ZIP, e i valori minimo e massimo tra gli anno-mese letti.
   */
  def readZip(): (RDD[ZipRzg1Metadata], String, String) = {
    val isRecoveryMode = Properties.isRecoveryMode

    val zipFiles = if (!isRecoveryMode) {
      val inputPath = Properties.getRzg1ZipInputPath
      val currentYear = Properties.getCurrentYear
      val currentMonth = Properties.getCurrentMonth
      flatAllZip(inputPath, currentYear, currentMonth)
    }
    else {
      Environment.spark
        .read
        .option("header", "true")
        .schema(CsvRecoverySchema.schema)
        .csv(Properties.getRecoveryCsvPath)
        .distinct
        .rdd
        .map(row => row.getAs[String](CsvRecoverySchema.file))
        .map(fileName => new File(fileName))
    }

    val zipFilesWithMetaData = getZipMetadata(zipFiles)

    val filteredZipFiles = if (!isRecoveryMode) zipFilesWithMetaData else filterNonexistentFiles(zipFilesWithMetaData)

    val yearMonthList = filteredZipFiles.filter(_.isAmmissibile).map(_.annoMeseCompetenza).persist(StorageLevel.MEMORY_AND_DISK)

    if (yearMonthList.isEmpty) (filteredZipFiles, YEARMONTH_MIN, YEARMONTH_MAX)
    else (filteredZipFiles, yearMonthList.min, yearMonthList.max)
  }

  /** A partire dal percorso esterno [[path]] e dall'anno-mese [[currentYear]], [[currentMonth]], ottiene la lista degli ZIP da leggere. */
  def flatAllZip(path: String, currentYear: String, currentMonth: String): RDD[File] = {
    val rootFolder = new File(path)
    // /CIG1_PivaDD
    val distributori = rootFolder.listFiles.toList.filter(_.isDirectory)

    val filesNumber = distributori.length / 100 + 4
    val defParalTwice = Environment.spark.sparkContext.defaultParallelism * 4
    val paral = if (filesNumber > defParalTwice) filesNumber else defParalTwice

    val sottesiRdd = Environment.spark.sparkContext.parallelize(distributori, paral)

    //CIG1_PivaDD/Anno/Mese
    sottesiRdd
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentYear))
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == currentMonth))
      .flatMap(_.listFiles().filter(f => f.isFile && f.getName.contains("RZG1")))
  }

  /** Estrae i metadati ed effettua il controllo sulla nomenclatura dello zip, per poi salvarli nella struttura [[ZipRzg1Metadata]]. */
  def getZipMetadata(zipFiles: RDD[File]): RDD[ZipRzg1Metadata] = {
    zipFiles
      .filter(_.getName.toLowerCase.endsWith(".zip"))
      .filter(_.getName.contains("RZG1"))   // to filtering out non RZG1 files (e.g. IZG1, since they are in the same folder)
      .map(file => {
        val pivaUtente = file.getParentFile.getParentFile.getParentFile.getName.replace(s"${CIG1}_", "")
        val year = file.getParentFile.getParentFile.getName.toLowerCase
        val month = file.getParentFile.getName.toLowerCase
        file.getName match {
          case zipFileRegex(piva1, piva2, annoMeseCompetenza, rzg, timestamp, progressivo) =>
            ZipRzg1Metadata(
              file = file,
              lastModified = file.lastModified(),
              pivaUtente = pivaUtente,
              pivaId = piva1,
              pivaUdd = piva2,
              yearDir = year,
              monthDir = month,
              annoMeseCompetenza = annoMeseCompetenza,
              timestamp = timestamp,
              progressivo = progressivo,
              isAmmissibile = true
            )
          case _ =>
            ZipRzg1Metadata(
              file = file,
              lastModified = file.lastModified(),
              pivaUtente = pivaUtente,
              pivaId = "",
              pivaUdd = "",
              yearDir = year,
              monthDir = month,
              annoMeseCompetenza = "",
              timestamp = "",
              progressivo = "",
              isAmmissibile = false,
              statusCode = COD_904,
              statusMessage = MOTIVAZIONE_NOME_ZIP
            )
        }
      })
  }

  def filterNonexistentFiles(zipFiles: RDD[ZipRzg1Metadata]): RDD[ZipRzg1Metadata] = {
    zipFiles.filter(_.lastModified != 0)
  }
}