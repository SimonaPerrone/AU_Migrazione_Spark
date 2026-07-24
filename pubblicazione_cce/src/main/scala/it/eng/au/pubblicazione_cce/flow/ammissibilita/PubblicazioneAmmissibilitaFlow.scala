package it.eng.au.pubblicazione_cce.flow.ammissibilita

import it.eng.au.pubblicazione_cce.dao.cce.CceRichiestaPodDao
import it.eng.au.pubblicazione_cce.file.csv.DataFrameCsvBuilder
import it.eng.au.pubblicazione_cce.file.writer.FileWriter
import it.eng.au.pubblicazione_cce.model.cce.{CceEsitoModel, CceRichiestaPodModel}
import it.eng.au.pubblicazione_cce.schema.cce.CceRichiestaPodSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate

trait PubblicazioneAmmissibilitaFlow extends Serializable {
  @transient private val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark
  import spark.implicits._

  // input
  val richiestePodDao = new CceRichiestaPodDao()

  // output
  val outputFileDao = new FileWriter()

  // properties
  val outputFilePath: String = Environment.getOutputFilePath
  val dataCalcolo: LocalDate = Environment.processDate
  val fileTimestamp: String = Environment.fileTimestamp

  // Parametri flusso
  val processo: String //P, Pr, ...
  val tipo: String // POD o FILE
  val dataRichieste: LocalDate // data lettura richieste
  val csvBuilder: DataFrameCsvBuilder // classe per creare CSV da salvare

  // Esegue trasformazioni per ritornare DataFrame esito
  protected def calcolaEsitoAmmissibilita(df: DataFrame): Dataset[CceEsitoModel]


  // Esegue flusso
  def run(): Dataset[CceEsitoModel] = {
    logger.warn(s"Inizio Pubblicazione file: $processo, $tipo")

    logger.warn(s"Lettura richieste: ${richiestePodDao.tableName}")
    val richieste = richiestePodDao.read()

    val richiesteProcessoFile = leggiRichieste(richieste)

    if (richiesteProcessoFile.isEmpty){
      logger.warn(s"Nessuna richiesta $processo, $tipo")
      return List.empty[CceEsitoModel].toDS()
    }


    val richiesteCsv = csvBuilder.computeCsvElements(richiesteProcessoFile.toDF())
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    logger.warn(s"Scrittura file ammissibilità in: $outputFilePath")
    val outputDs = csvBuilder.dfToFileModel(richiesteCsv)
    outputFileDao.write(outputDs)

    val esitoAmmissiblita = calcolaEsitoAmmissibilita(richiesteCsv)

    richiesteCsv.unpersist()

    esitoAmmissiblita
  }

  /** *
   * Legge richieste da "richiestePodDao" filtrando per data richiesta, tipo processo e tipo richiesta
   */
  protected def leggiRichieste(richieste: Dataset[CceRichiestaPodModel]): Dataset[CceRichiestaPodModel] = {
    richieste
      .where(col(CceRichiestaPodSchema.partition_request_date) === dataRichieste.toString)
      .where(col(CceRichiestaPodSchema.t_processo) === processo)
      .where(col(CceRichiestaPodSchema.t_tipo_amm) === tipo)
  }

}
