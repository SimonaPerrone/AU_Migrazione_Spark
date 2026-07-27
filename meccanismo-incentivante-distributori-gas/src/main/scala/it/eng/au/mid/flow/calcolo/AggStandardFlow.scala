package it.eng.au.mid.flow.calcolo

import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.dao.file.csv.calcolo.{EsclusioniAggDao, InclusioniAggDao}
import it.eng.au.mid.dao.hive.agg.{DailyConsumptionAggDao, DailyConsumptionAggEsclusiDao, DailyConsumptionAggIncoerentiDao}
import it.eng.au.mid.dao.hive.mid.MidContatoriDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.Flow
import it.eng.au.mid.flow.calcolo.CalcoloMidFunzioni.calcoloMaxExexIdTracciaturaPerAnnomese
import it.eng.au.mid.model.file.calcolo.{EsclusioniModel, InclusioniModel}
import it.eng.au.mid.model.flow.calcolo.AnnoMeseExecusionIdModel
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.file.calcolo.{EsclusioniSchema, InclusioniSchema}
import it.eng.au.mid.schema.flow.calcolo.AnnoMeseExecutionIdSchema
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Date
import java.time.LocalDate
import scala.collection.mutable

class AggStandardFlow extends Flow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  override val flowName: String = "AGG Standard"

  // input
  val incoerentiDao = new DailyConsumptionAggIncoerentiDao
  val esclusiDao = new DailyConsumptionAggEsclusiDao
  val fileEsclusioni = new EsclusioniAggDao
  val fileInclusioni = new InclusioniAggDao
  val dailyConsumptionDao = new DailyConsumptionAggDao

  // output
  val midContatoriDao = new MidContatoriDao

  //parametri
  val aggExecutionId: Long = Environment.getProperty("job.params.executionid_agg").toLong
  val executionId: Long = Environment.executionId
  val dataCalcolo: LocalDate = Environment.processDate
  val fileEsclusioniAbilitato: Boolean = if (Environment.getProperty("job.params.exclusion_agg_enabled").toUpperCase == "TRUE") true else false
  val fileInclusioniAbilitato: Boolean = if (Environment.getProperty("job.params.inclusion_agg_enabled").toUpperCase == "TRUE") true else false

  /** *
   * A differenza del processo SBG che utilizza un solo annomese, il processo AGG prende un execution id di calcolo
   * ed estrae tutti i mesi calcolati per quella esecuzione
   */
  override def run(): Unit = {
    logger.warn(s"Inizio processo: $flowName")
    logger.warn(s"ExecutionId: $executionId")
    logger.warn(s"dataCalcolo: $dataCalcolo")
    logger.warn(s"aggExecutionId: $aggExecutionId")
    logger.warn(s"fileEsclusioniAbilitato: $fileEsclusioniAbilitato")
    logger.warn(s"fileInclusioniAbilitato: $fileInclusioniAbilitato")

    // calcolo annomese da elaborare estraendo valori a partire da execution id
    logger.warn(s"Estrazione informazioni ultimo calcolo da: ${dailyConsumptionDao.tableName} con executionId: $aggExecutionId")
    val infoEsecuzione = dailyConsumptionDao.infoUltimaEsecuzione(aggExecutionId)
    val sessioneEsecuzione = infoEsecuzione("sessione").asInstanceOf[String]
    val tracciatoEsecuzione = infoEsecuzione("tracciatura").asInstanceOf[String]
    val executionIdEsecuzione = infoEsecuzione("executionId").asInstanceOf[Long]
    val annomeseAnomaliList = infoEsecuzione("annomeseAnomali").asInstanceOf[mutable.WrappedArray[String]].toList
    logger.warn(s"sessioneEsecuzione: $sessioneEsecuzione")
    logger.warn(s"tracciatoEsecuzione: $tracciatoEsecuzione")
    logger.warn(s"executionIdEsecuzione: $executionIdEsecuzione")
    logger.warn(s"annomeseAnomaliList: ${annomeseAnomaliList.mkString(", ")}")

    // lettura dati inclusioni ed esclusioni
    logger.warn("Predisposizione file inclusioni ed esclusioni")
    val esclusioniDs = if (fileEsclusioniAbilitato) {
      logger.warn(s"Lettura file esclusioni ${fileEsclusioni.path}")
      val tmpEsclusioni = fileEsclusioni.read().where(col(EsclusioniSchema.annomese).isin(annomeseAnomaliList: _*))
      logger.warn(tmpEsclusioni.collect().mkString("\n", "\n", ""))
      tmpEsclusioni
    } else {
      logger.warn(s"Esclusioni disabilitate")
      List.empty[EsclusioniModel].toDS()
    }

    val inclusioniDs = if (fileInclusioniAbilitato) {
      logger.warn(s"Lettura file inclusioni ${fileInclusioni.path}")
      val tmpInclusioni = fileInclusioni.read().where(col(InclusioniSchema.annomese).isin(annomeseAnomaliList: _*))
      logger.warn(tmpInclusioni.collect().mkString("\n", "\n", ""))
      tmpInclusioni
    } else {
      logger.warn(s"Inclusioni disabilitate")
      List.empty[InclusioniModel].toDS()
    }

    // lettura dati input
    logger.warn(s"Lettura anomali: ${incoerentiDao.tableName}")
    val incoerentiDs = incoerentiDao.read()
    logger.warn(s"Lettura anomali: ${esclusiDao.tableName}")
    val esclusiDs = esclusiDao.read()
    logger.warn(s"Lettura MID versione precedente: ${midContatoriDao.tableName}")
    val midContatoriDs = midContatoriDao.read()
    logger.warn(s"Lettura consumi giornalieri: ${dailyConsumptionDao.tableName}")
    val dailyConsumption = dailyConsumptionDao.read()

    // Calcolo anomali da tabelle esclusi e incoerenti
    val anomaliDs = CalcoloMidFunzioniAgg.leggiPdrAnomaliAgg(incoerentiDs, esclusiDs, executionIdEsecuzione, annomeseAnomaliList)
    val anomaliFiltratiDs = CalcoloMidFunzioni.rimuoviPdrDaEscludereDalCalcolo(anomaliDs, esclusioniDs, inclusioniDs)
    val anomaliCalcolatiDs = CalcoloMidFunzioni.raggruppaPdrAnomali(anomaliFiltratiDs)

    // Calcolo dati PDR presenti in tabella MID nell'ultima versione
    val midContatoriPrecedenti = leggiAnomaliMidPrecedenti(midContatoriDs, annomeseAnomaliList)
    // Rimuovi inclusioni ed esclusioni
    val midContatoriPrecedenteFiltrati = CalcoloMidFunzioni.rimuoviInclusioniEdEsclusioni(midContatoriPrecedenti, inclusioniDs, esclusioniDs).cache()

    val executionIdPrevDs =CalcoloMidFunzioni.calcoloMaxExexIdTracciaturaPerAnnomese(midContatoriPrecedenteFiltrati)

    // trasformazione dati
    val midContatori = CalcoloMidFunzioni.calcolaContatoriMid(anomaliCalcolatiDs, midContatoriPrecedenteFiltrati, executionIdPrevDs)
    // aggiungi inclusioni
    val midContatoriConInclusioni = CalcoloMidFunzioni.aggiungiInclusioni(midContatori, inclusioniDs, executionIdPrevDs)
    // aggiungi esclusioni
    val midContatoriConInclusioniEdEsclusioni = CalcoloMidFunzioni.aggiungiEsclusioni(midContatoriConInclusioni, esclusioniDs, executionIdPrevDs)
    // aggiunge valore treatment e completa struttura
    val midContatoriFinale = CalcoloMidFunzioni.finalizzaMidContatori(
      mid = midContatoriConInclusioniEdEsclusioni,
      dailyConsumption = dailyConsumption,
      dataCalcolo = Date.valueOf(dataCalcolo),
      processoTracciatura = sessioneEsecuzione,
      sessioneTracciatura = tracciatoEsecuzione,
      tipoCalcolo = CostantiMid.TIPO_CALCOLO_ORDINARIO,
      executionIdDaily = executionIdEsecuzione,
      executionId = executionId
    )

    // scrittura su output
    logger.warn(s"Scrittura dati in tabella ${midContatoriDao.tableName}")
    midContatoriDao.write(midContatoriFinale)
    logger.warn(s"Fine processo: $flowName")
  }

  /////////////////////////////////////////////////////////

  /** *
   * Leggi anomali su precedente calcolo MID per lista annomese
   */
  def leggiAnomaliMidPrecedenti(mid: Dataset[MidContatoriModel], annomeseLista: List[String]): Dataset[MidContatoriModel] = {
    val rowNumberCol = "_tmp_rn"
    mid
      .where(col(MidContatoriSchema.annomese).isin(annomeseLista: _*))
      .where(col(MidContatoriSchema.stato).isin(CostantiMid.STATO_VALIDO, CostantiMid.STATO_FORZATO))
      .withColumn(rowNumberCol, row_number().over(Window.partitionBy(MidContatoriSchema.pdr, MidContatoriSchema.annomese)
        .orderBy(col(MidContatoriSchema.executionid_tracciatura).desc)))
      .where(col(rowNumberCol) === 1)
      .selectExpr(MidContatoriSchema.getValues: _*)
      .as[MidContatoriModel]
  }

}