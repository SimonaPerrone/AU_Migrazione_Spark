package it.eng.au.mid.flow.pubblicazione

import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.dao.file.csv.pubblicazione._
import it.eng.au.mid.dao.hive.agg.DailyConsumptionAggDao
import it.eng.au.mid.dao.hive.mid.{Mid1DettaglioDao, MidContatoriDao}
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.Flow
import it.eng.au.mid.model.file.pubblicazione.MidAlphaValoriModel
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidContatoriModel}
import it.eng.au.mid.schema.file.pubblicazione._
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionSchema
import it.eng.au.mid.schema.hive.mid.{Mid1DettaglioSchema, MidContatoriSchema}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit, max, not}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDate


/** *
 * Calcola tabella mid1DettaglioDao per periodo compreso tra annoMeseDa e annoMeseA
 */
class PredisposizioneMid1Flow extends Flow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  override val flowName: String = "Predisposizione MID1"

  //input
  val midContatoriDao = new MidContatoriDao
  val dailyConsumptionAggDao = new DailyConsumptionAggDao
  //output
  val mid1DettaglioDao = new Mid1DettaglioDao

  // file valori alpha
  val fileAlphaValori = new MidAlphaValoriDao

  // file esclusioni
  val fileEsclusioniPdrDao = new Mid1EsclusioniPdrDao
  val fileEsclusioniTrattamentoDao = new Mid1EsclusioniTrattamentoDao
  val fileEsclusioniAnnomeseDao = new Mid1EsclusioniAnnomeseDao
  val fileEsclusioniDistributoreDao = new Mid1EsclusioniDistributoreDao

  // parametri job
  val midAnnomeseDa: String = Environment.getProperty("job.param.mid1_annomese_da")
  val midAnnomeseA: String = Environment.getProperty("job.param.mid1_annomese_a")
  val sogliaContatore: Int = Environment.getProperty("job.param.mid1_soglia").toInt

  // parametri environment
  val executionId: Long = Environment.executionId
  val dataCalcolo: LocalDate = Environment.processDate

  override def run(): Unit = {
    logger.warn(s"Inizio processo: $flowName")

    logger.warn(s"dataCalcolo: $dataCalcolo")
    logger.warn(s"executionId: $executionId")
    logger.warn(s"midAnnomeseDa: $midAnnomeseDa")
    logger.warn(s"midAnnomeseA: $midAnnomeseA")
    logger.warn(s"sogliaContatore: $sogliaContatore")

    // LETTURA DATI INPUT
    logger.warn(s"Lettura mid contatori: ${midContatoriDao.tableName}")
    val midContatoriDs = midContatoriDao.read()

    logger.warn(s"Lettura dailyConsumption: ${dailyConsumptionAggDao.tableName}")
    val dailyConsumptionAggDs = dailyConsumptionAggDao.read()

    logger.warn("Lettura file esclusioni")
    logger.warn(s"Esclusioni PDR: ${fileEsclusioniPdrDao.path}")
    val fileEsclusioniPdr = fileEsclusioniPdrDao.read()
      .where(col(MidEsclusioniPdrSchema.pdr).isNotNull)
      .collect().map(_.pdr).toList
    logger.warn(s"${
      if (fileEsclusioniPdr.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniPdr.mkString(", ")
      }
    }")

    logger.warn(s"Esclusioni Trattamento: ${fileEsclusioniTrattamentoDao.path}")
    val fileEsclusioniTrattamento = fileEsclusioniTrattamentoDao.read()
      .where(col(MidEsclusioniTrattamentoSchema.trattamento).isNotNull)
      .collect().map(_.trattamento).toList
    logger.warn(s"${
      if (fileEsclusioniTrattamento.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniTrattamento.mkString(", ")
      }
    }")

    logger.warn(s"Esclusioni Annomese: ${fileEsclusioniAnnomeseDao.path}")
    val fileEsclusioniAnnomese = fileEsclusioniAnnomeseDao.read()
      .where(col(MidEsclusioniAnnomeseSchema.annomese).isNotNull)
      .collect().map(_.annomese).toList
    logger.warn(s"${
      if (fileEsclusioniAnnomese.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniAnnomese.mkString(", ")
      }
    }")

    logger.warn(s"Esclusioni Distributori: ${fileEsclusioniDistributoreDao.path}")
    val fileEsclusioniDistributore = fileEsclusioniDistributoreDao.read()
      .where(col(MidEsclusioniDistributoreSchema.distributore).isNotNull)
      .collect().map(_.distributore).toList
    logger.warn(s"${
      if (fileEsclusioniDistributore.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniDistributore.mkString(", ")
      }
    }")

    logger.warn(s"Valori Alpha: ${fileAlphaValori.path}")
    val alphaValoriDs = fileAlphaValori.read()
    logger.warn(alphaValoriDs.show(truncate = false))

    // ELABORAZIONE DATI
    val midContatoriFiltrati = PredisposizioneFunzioni.filtraMidContatoriDaAnalizzare(midContatoriDs, midAnnomeseDa, midAnnomeseA, sogliaContatore)

    val midEsclusiAnnomese = PredisposizioneFunzioni.escludiElementi(midContatoriFiltrati, MidContatoriSchema.annomese, fileEsclusioniAnnomese)
    val midEsclusiPdr = PredisposizioneFunzioni.escludiElementi(midEsclusiAnnomese, MidContatoriSchema.pdr, fileEsclusioniPdr)
    val midEsclusiTrattamenti = PredisposizioneFunzioni.escludiElementi(midEsclusiPdr, MidContatoriSchema.treatment, fileEsclusioniTrattamento)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val dailyConsumptionAggDsFiltrati = PredisposizioneFunzioni.filtraDailyConsumption(dailyConsumptionAggDs, midEsclusiTrattamenti)

    val midContatoriArricchiti = PredisposizioneFunzioni.aggiungiInformazioniAnagrafica(midEsclusiTrattamenti, dailyConsumptionAggDsFiltrati, executionId)

    val midEsclusiDistributori = PredisposizioneFunzioni.escludiDistributori(midContatoriArricchiti, fileEsclusioniDistributore)

    val midDettaglio = PredisposizioneFunzioni.calcolaAlpha(midEsclusiDistributori, alphaValoriDs)

    // SCRITTURA TABELLA FINALE
    logger.warn(s"Scrittura tabella: ${mid1DettaglioDao.tableName}")
    mid1DettaglioDao.write(midDettaglio)

    logger.warn(s"Fine processo: $flowName")
  }

}
