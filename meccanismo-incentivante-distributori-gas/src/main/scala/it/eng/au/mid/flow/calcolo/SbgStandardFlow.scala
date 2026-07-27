package it.eng.au.mid.flow.calcolo

import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.dao.file.csv.calcolo.{EsclusioniSbgDao, InclusioniSbgDao}
import it.eng.au.mid.dao.hive.mid.MidContatoriDao
import it.eng.au.mid.dao.hive.sbg.{DailyConsumptionSbgDao, DailyConsumptionSbgEsclusiDao, DailyConsumptionSbgIncoerentiDao}
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.Flow
import it.eng.au.mid.model.file.calcolo.{EsclusioniModel, InclusioniModel}
import it.eng.au.mid.model.flow.calcolo.{DailyConsumptionEsclusiModel, DailyConsumptionIncoerentiModel, PdrAnomaloModel}
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.file.calcolo.InclusioniSchema
import it.eng.au.mid.schema.flow.calcolo.{DailyConsumptionEsclusiSchema, DailyConsumptionIncoerentiSchema, PdrAnomaloSchema}
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SbgStandardFlow extends Flow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  override val flowName: String = "SBG Standard"

  // input
  val incoerentiDao = new DailyConsumptionSbgIncoerentiDao
  val esclusiDao = new DailyConsumptionSbgEsclusiDao
  val fileEsclusioni = new EsclusioniSbgDao
  val fileInclusioni = new InclusioniSbgDao
  val dailyConsumptionDao = new DailyConsumptionSbgDao

  // output
  val midContatoriDao = new MidContatoriDao

  //parametri
  val meseOffset: Int = Environment.getProperty("job.params.offset_sbg").toInt
  val executionId: Long = Environment.executionId
  val dataCalcolo: LocalDate = Environment.processDate
  val fileEsclusioniAbilitato: Boolean = if (Environment.getProperty("job.params.exclusion_sbg_enabled").toUpperCase == "TRUE") true else false
  val fileInclusioniAbilitato: Boolean = if (Environment.getProperty("job.params.inclusion_sbg_enabled").toUpperCase == "TRUE") true else false

  override def run(): Unit = {
    logger.warn(s"Inizio processo: $flowName")
    // parametri
    logger.warn(s"ExecutionId: $executionId")
    logger.warn(s"dataCalcolo: $dataCalcolo")
    logger.warn(s"fileEsclusioniAbilitato: $fileEsclusioniAbilitato")
    logger.warn(s"fileInclusioniAbilitato: $fileInclusioniAbilitato")
    // calcolo annomese da elaborare come data odierna - mese offset
    val annomeseAnomali = dataCalcolo.minusMonths(meseOffset)
      .format(DateTimeFormatter.ofPattern("yyyyMM"))
    logger.warn(s"Annomese di calcolo: $annomeseAnomali ($dataCalcolo - $meseOffset mesi)")

    logger.warn(s"Estrazione informazioni ultimo calcolo da: ${dailyConsumptionDao.tableName}")
    val infoEsecuzione = dailyConsumptionDao.infoUltimaEsecuzione(annomeseAnomali)
    val sessioneEsecuzione = infoEsecuzione("sessione").asInstanceOf[String]
    val tracciatoEsecuzione = infoEsecuzione("tracciatura").asInstanceOf[String]
    val executionIdEsecuzione = infoEsecuzione("executionId").asInstanceOf[Long]
    logger.warn(s"sessioneEsecuzione: $sessioneEsecuzione")
    logger.warn(s"tracciatoEsecuzione: $tracciatoEsecuzione")
    logger.warn(s"executionIdEsecuzione: $executionIdEsecuzione")

    // lettura dati inclusioni ed esclusioni
    logger.warn("Predisposizione file inclusioni ed esclusioni")
    val esclusioniDs = if (fileEsclusioniAbilitato) {
      logger.warn(s"Lettura file esclusioni ${fileEsclusioni.path}")
      val tmpEsclusioni = fileEsclusioni.read().where(col(InclusioniSchema.annomese) === annomeseAnomali)
      logger.warn(tmpEsclusioni.collect().mkString("\n", "\n", ""))
      tmpEsclusioni
    } else {
      logger.warn(s"Esclusioni disabilitate")
      List.empty[EsclusioniModel].toDS()
    }

    val inclusioniDs = if (fileInclusioniAbilitato) {
      logger.warn(s"Lettura file inclusioni ${fileInclusioni.path}")
      val tmpInclusioni = fileInclusioni.read().where(col(InclusioniSchema.annomese) === annomeseAnomali)
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
    val anomaliDs = leggiPdrAnomaliSbg(incoerentiDs, esclusiDs, executionIdEsecuzione, annomeseAnomali)
    val anomaliFiltratiDs = CalcoloMidFunzioni.rimuoviPdrDaEscludereDalCalcolo(anomaliDs, esclusioniDs, inclusioniDs)
    val anomaliCalcolatiDs = CalcoloMidFunzioni.raggruppaPdrAnomali(anomaliFiltratiDs)

    // Calcolo dati PDR presenti in tabella MID nell'ultima versione
    val midContatoriPrecedenti = leggiAnomaliMidPrecedenti(midContatoriDs, annomeseAnomali).cache()
    val executionIdPrev = if (midContatoriPrecedenti.isEmpty) {
      null
    } else {
      midContatoriPrecedenti.head().executionid_tracciatura
    }
    // Rimuovi inclusioni ed esclusioni
    val midContatoriPrecedenteFiltrati = CalcoloMidFunzioni.rimuoviInclusioniEdEsclusioni(midContatoriPrecedenti, inclusioniDs, esclusioniDs)

    // trasformazione dati
    val midContatori = CalcoloMidFunzioni.calcolaContatoriMid(anomaliCalcolatiDs, midContatoriPrecedenteFiltrati, executionIdPrev)
    // aggiungi inclusioni
    val midContatoriConInclusioni = CalcoloMidFunzioni.aggiungiInclusioni(midContatori, inclusioniDs, executionIdPrev)
    // aggiungi esclusioni
    val midContatoriConInclusioniEdEsclusioni = CalcoloMidFunzioni.aggiungiEsclusioni(midContatoriConInclusioni, esclusioniDs, executionIdPrev)
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

  /** *
   * Legge PDR anomali da tabelle input [[incoerentiDao]] e [[esclusiDao]] per [[executionId]] passato
   */
  def leggiPdrAnomaliSbg(incoerentiDs: Dataset[DailyConsumptionIncoerentiModel],
                         esclusiDs: Dataset[DailyConsumptionEsclusiModel],
                         executionId: Long, annomese: String): Dataset[PdrAnomaloModel] = {
    val incoerentiAnomaliDs = incoerentiDs
      .where(col(DailyConsumptionIncoerentiSchema.executionid) === executionId)
      .where(col(DailyConsumptionIncoerentiSchema.annomese) === annomese)
      .where(col(DailyConsumptionIncoerentiSchema.ispdranomalousgdm) === true)
      .select(
        DailyConsumptionIncoerentiSchema.pdr,
        DailyConsumptionIncoerentiSchema.annomese,
        DailyConsumptionIncoerentiSchema.executionid
      )
      .distinct()
      .withColumn(PdrAnomaloSchema.tipoAnomalia, lit(CostantiMid.CAUSALE_INCOERENTI))
      .selectExpr(PdrAnomaloSchema.getValues: _*)
      .as[PdrAnomaloModel]

    val esclusiAnomaliDs = esclusiDs
      .where(col(DailyConsumptionEsclusiSchema.executionid) === executionId)
      .where(col(DailyConsumptionEsclusiSchema.annomese) === annomese)
      .select(
        DailyConsumptionEsclusiSchema.pdr,
        DailyConsumptionEsclusiSchema.annomese,
        DailyConsumptionEsclusiSchema.executionid
      )
      .distinct()
      .withColumn(PdrAnomaloSchema.tipoAnomalia, lit(CostantiMid.CAUSALE_ESCLUSI))
      .selectExpr(PdrAnomaloSchema.getValues: _*)
      .as[PdrAnomaloModel]

    incoerentiAnomaliDs
      .union(esclusiAnomaliDs)
  }

  /** *
   * Leggi anomali su precedente calcolo MID per dato anno mese
   */
  def leggiAnomaliMidPrecedenti(mid: Dataset[MidContatoriModel], annomese: String): Dataset[MidContatoriModel] = {
    val executionIdDf = CalcoloMidFunzioni.maxExecutionIdPerAnnomese(
      df = mid.toDF(),
      annomese = annomese,
      annomeseCol = MidContatoriSchema.annomese,
      executionIdCol = MidContatoriSchema.executionid_tracciatura
    )
    executionIdDf match {
      case Some(value) => mid
        .where(col(MidContatoriSchema.executionid_tracciatura) === value)
        .where(col(MidContatoriSchema.annomese) === annomese)
        .where(col(MidContatoriSchema.stato).isin(CostantiMid.STATO_VALIDO, CostantiMid.STATO_FORZATO))
        .selectExpr(MidContatoriSchema.getValues: _*)
        .as[MidContatoriModel]
      case None => List.empty[MidContatoriModel].toDS
    }
  }

}
