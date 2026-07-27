package it.eng.au.mid.flow.calcolo

import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.calcolo.{EsclusioniModel, InclusioniModel}
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.flow.calcolo.{AnnoMeseExecusionIdModel, PdrAnomaloModel}
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.file.calcolo.{EsclusioniSchema, InclusioniSchema}
import it.eng.au.mid.schema.flow.calcolo.{AnnoMeseExecutionIdSchema, DailyConsumptionSchema, PdrAnomaloSchema}
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import java.sql.Date

/***
 * Funzioni in comune tra i processi SBG e AGG
 */
object CalcoloMidFunzioni {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Ritorna il valore massimo di executionId per un dato anno-mese
   *
   * @param annomeseCol    : nome colonna contenente valori annomese
   * @param executionIdCol : nome colonna contenente valori executionId
   */
  def maxExecutionIdPerAnnomese(df: DataFrame, annomese: String, annomeseCol: String = "annomese", executionIdCol: String = "executionid"): Option[Long] = {
    val colName = "_tmp_max"
    val res = df
      .where(col(annomeseCol) === annomese)
      .groupBy(col(annomeseCol))
      .agg(functions.max(col(executionIdCol)).as(executionIdCol))
      .select(executionIdCol)
      .cache()

    if (res.isEmpty) {
      None
    } else {
      val executionId = res
        .agg(functions.max(executionIdCol).as(colName))
        .head
        .getAs[Long](colName)
      Some(executionId)
    }
  }

  /** *
   * Rimuove PDR da non calcolare presenti nel file inclusioni ed esclusioni. Le inclusioni e le esclusioni devono essere aggiunte
   * dopo il calcolo
   */
  def rimuoviPdrDaEscludereDalCalcolo(anomali: Dataset[PdrAnomaloModel], esclusioni: Dataset[EsclusioniModel],
                                      inclusioni: Dataset[InclusioniModel]): Dataset[PdrAnomaloModel] = {
    anomali
      .join(esclusioni,
        anomali(PdrAnomaloSchema.pdr) === esclusioni(EsclusioniSchema.pdr)
          and anomali(PdrAnomaloSchema.annomese) === esclusioni(EsclusioniSchema.annomese),
        "LEFT_ANTI")
      .join(inclusioni,
        anomali(PdrAnomaloSchema.pdr) === inclusioni(InclusioniSchema.pdr)
          and anomali(PdrAnomaloSchema.annomese) === inclusioni(InclusioniSchema.annomese),
        "LEFT_ANTI")
      .selectExpr(PdrAnomaloSchema.getValues: _*)
      .as[PdrAnomaloModel]
  }

  /** *
   * Raggruppa PDR se presenti piu' volte negli anomali. Valorizza la colonna [[PdrAnomaloSchema.tipoAnomalia]] con il valore
   * finale per la colonna [[MidContatoriSchema.causale_tracciatura]]
   */
  def raggruppaPdrAnomali(pdrAnomali: Dataset[PdrAnomaloModel]): Dataset[PdrAnomaloModel] = {
    val listaAnomalieCol = "_tmp_lista_anomalie"
    pdrAnomali
      .groupBy(PdrAnomaloSchema.pdr, PdrAnomaloSchema.annomese, PdrAnomaloSchema.executionid)
      .agg(collect_set(PdrAnomaloSchema.tipoAnomalia).as(listaAnomalieCol))
      .withColumn(PdrAnomaloSchema.tipoAnomalia,
        when(array_contains(col(listaAnomalieCol), CostantiMid.CAUSALE_ESCLUSI) and array_contains(col(listaAnomalieCol), CostantiMid.CAUSALE_INCOERENTI), CostantiMid.CAUSALE_ESCLUSI_INCOERENTI)
          .when(array_contains(col(listaAnomalieCol), CostantiMid.CAUSALE_ESCLUSI), CostantiMid.CAUSALE_ESCLUSI)
          .otherwise(CostantiMid.CAUSALE_INCOERENTI)
      )
      .selectExpr(PdrAnomaloSchema.getValues: _*)
      .as[PdrAnomaloModel]
  }


  /**
   * Rimuove coppier PDR-annomese da midContatori se presenti in inclusioni ed esclusioni.
   * Queste informazioni devono essere calcolate in seguite con logiche specifiche
   */
  def rimuoviInclusioniEdEsclusioni(midContatori: Dataset[MidContatoriModel], inclusioni: Dataset[InclusioniModel],
                                    esclusioni: Dataset[EsclusioniModel]): Dataset[MidContatoriModel] = {
    midContatori.join(
      inclusioni,
      midContatori(MidContatoriSchema.pdr) === inclusioni(InclusioniSchema.pdr)
        and midContatori(MidContatoriSchema.annomese) === inclusioni(InclusioniSchema.annomese),
      "LEFT_ANTI"
    ).join(
      esclusioni,
      midContatori(MidContatoriSchema.pdr) === esclusioni(EsclusioniSchema.pdr)
        and midContatori(MidContatoriSchema.annomese) === esclusioni(EsclusioniSchema.annomese),
      "LEFT_ANTI"
    ).as[MidContatoriModel]
  }

  /** *
   * Motore di calcolo contatore SBG
   * Calcola contatori tra pdrAnomali e midContatoriPrecedente.
   * Valorizza pdr, contatore, stato, causale_tracciatura, executionid_tracciatura_prev, annomese
   * Se PDR è presente su pdrAnomali e su mid allora valore contatore = contatore mid + 1 e stato VALIDO
   * Se PDR è presente su pdrAnomali ma non su mid allora contatore uguale a 1 e stato VALIDO
   * Se PDR non è presente su pdrAnomali ma è presente su mid allora stato INVALIDO
   * causale_tracciatura: rimane il valore presente su anomalo
   * executionid_tracciatura_prev: prente executionid_tracciatura di mid
   */
  def calcolaContatoriMid(pdrAnomali: Dataset[PdrAnomaloModel], midContatoriPrecedente: Dataset[MidContatoriModel],
                          executionIdPrev: java.lang.Long): Dataset[MidContatoriModel] = {
    // verifica se esiste PDR in conteggio nel calcolo precedente, nel caso contatore = precedente + 1
    val pdrAncoraAnomali = pdrAnomali.join(
      midContatoriPrecedente,
      pdrAnomali(PdrAnomaloSchema.pdr) === midContatoriPrecedente(MidContatoriSchema.pdr)
        and pdrAnomali(PdrAnomaloSchema.annomese) === midContatoriPrecedente(MidContatoriSchema.annomese),
      "LEFT")
      .select(
        pdrAnomali(PdrAnomaloSchema.pdr) as MidContatoriSchema.pdr,
        coalesce(midContatoriPrecedente(MidContatoriSchema.contatore), lit(0)) + 1 as MidContatoriSchema.contatore,
        lit(CostantiMid.STATO_VALIDO) as MidContatoriSchema.stato,
        lit(null) as MidContatoriSchema.treatment,
        lit(null) as MidContatoriSchema.data_tracciatura,
        lit(null) as MidContatoriSchema.processo_tracciatura,
        lit(null) as MidContatoriSchema.sessione_tracciatura,
        pdrAnomali(PdrAnomaloSchema.tipoAnomalia) as MidContatoriSchema.causale_tracciatura,
        lit(null) as MidContatoriSchema.tipo_calcolo,
        lit(null) as MidContatoriSchema.executionid_daily_consumption,
        lit(executionIdPrev) as MidContatoriSchema.executionid_tracciatura_prev,
        pdrAnomali(PdrAnomaloSchema.annomese) as MidContatoriSchema.annomese,
        lit(null) as MidContatoriSchema.executionid_tracciatura
      )
      .as[MidContatoriModel]

    // verifica se esiste un PDR in conteggio calcolo precedente ma non esiste in attuale, nel caso invalida
    val pdrNonPiuAnomali = midContatoriPrecedente.join(
      pdrAnomali,
      midContatoriPrecedente(MidContatoriSchema.pdr) === pdrAnomali(PdrAnomaloSchema.pdr)
        and midContatoriPrecedente(MidContatoriSchema.annomese) === pdrAnomali(PdrAnomaloSchema.annomese),
      "LEFT_ANTI")
      .select(
        midContatoriPrecedente(MidContatoriSchema.pdr) as MidContatoriSchema.pdr,
        midContatoriPrecedente(MidContatoriSchema.contatore) as MidContatoriSchema.contatore,
        lit(CostantiMid.STATO_INVALIDO) as MidContatoriSchema.stato,
        midContatoriPrecedente(MidContatoriSchema.treatment) as MidContatoriSchema.treatment,
        lit(null) as MidContatoriSchema.data_tracciatura,
        lit(null) as MidContatoriSchema.processo_tracciatura,
        lit(null) as MidContatoriSchema.sessione_tracciatura,
        midContatoriPrecedente(MidContatoriSchema.causale_tracciatura) as MidContatoriSchema.causale_tracciatura,
        lit(null) as MidContatoriSchema.tipo_calcolo,
        lit(null) as MidContatoriSchema.executionid_daily_consumption,
        lit(executionIdPrev) as MidContatoriSchema.executionid_tracciatura_prev,
        midContatoriPrecedente(MidContatoriSchema.annomese) as MidContatoriSchema.annomese,
        lit(null) as MidContatoriSchema.executionid_tracciatura
      )
      .as[MidContatoriModel]

    pdrAncoraAnomali.union(pdrNonPiuAnomali)
  }

  def calcoloMaxExexIdTracciaturaPerAnnomese(midContatori:  Dataset[MidContatoriModel])
                          : Dataset[AnnoMeseExecusionIdModel] = {
    midContatori
    .groupBy(MidContatoriSchema.annomese)
      .agg(max(MidContatoriSchema.executionid_tracciatura).alias(AnnoMeseExecutionIdSchema.executionid_max))
      .withColumnRenamed(MidContatoriSchema.annomese,AnnoMeseExecutionIdSchema.annomese_riferimento)
      .as[AnnoMeseExecusionIdModel]
  }

  /** *
   * Motore di calcolo contatore AGG (Standard / back in time)
   * Calcola contatori tra pdrAnomali e midContatoriPrecedente.
   * Valorizza pdr, contatore, stato, causale_tracciatura, executionid_tracciatura_prev, annomese
   * Se PDR è presente su pdrAnomali e su mid allora valore contatore = contatore mid + 1 e stato VALIDO
   * Se PDR è presente su pdrAnomali ma non su mid allora contatore uguale a 1 e stato VALIDO
   * Se PDR non è presente su pdrAnomali ma è presente su mid allora stato INVALIDO
   * causale_tracciatura: rimane il valore presente su anomalo
   * executionid_tracciatura_prev: prente executionid_tracciatura di mid per quel annomese
   */
  def calcolaContatoriMid(pdrAnomali: Dataset[PdrAnomaloModel], midContatoriPrecedente: Dataset[MidContatoriModel],
                          executionIdPrevDs: Dataset[AnnoMeseExecusionIdModel]): Dataset[MidContatoriModel] = {
    // verifica se esiste PDR in conteggio nel calcolo precedente, nel caso contatore = precedente + 1
    val pdrAncoraAnomali = pdrAnomali.join(
      midContatoriPrecedente,
      pdrAnomali(PdrAnomaloSchema.pdr) === midContatoriPrecedente(MidContatoriSchema.pdr)
        and pdrAnomali(PdrAnomaloSchema.annomese) === midContatoriPrecedente(MidContatoriSchema.annomese),
      "LEFT")
      .join(executionIdPrevDs,executionIdPrevDs(AnnoMeseExecutionIdSchema.annomese_riferimento) === pdrAnomali(PdrAnomaloSchema.annomese),
        "LEFT")
      .select(
        pdrAnomali(PdrAnomaloSchema.pdr) as MidContatoriSchema.pdr,
        coalesce(midContatoriPrecedente(MidContatoriSchema.contatore), lit(0)) + 1 as MidContatoriSchema.contatore,
        lit(CostantiMid.STATO_VALIDO) as MidContatoriSchema.stato,
        lit(null) as MidContatoriSchema.treatment,
        lit(null) as MidContatoriSchema.data_tracciatura,
        lit(null) as MidContatoriSchema.processo_tracciatura,
        lit(null) as MidContatoriSchema.sessione_tracciatura,
        pdrAnomali(PdrAnomaloSchema.tipoAnomalia) as MidContatoriSchema.causale_tracciatura,
        lit(null) as MidContatoriSchema.tipo_calcolo,
        lit(null) as MidContatoriSchema.executionid_daily_consumption,
        executionIdPrevDs(AnnoMeseExecutionIdSchema.executionid_max) as MidContatoriSchema.executionid_tracciatura_prev,
        pdrAnomali(PdrAnomaloSchema.annomese) as MidContatoriSchema.annomese,
        lit(null) as MidContatoriSchema.executionid_tracciatura
      )
      .as[MidContatoriModel]

    // verifica se esiste un PDR in conteggio calcolo precedente ma non esiste in attuale, nel caso invalida
    val pdrNonPiuAnomali = midContatoriPrecedente.join(
      pdrAnomali,
      midContatoriPrecedente(MidContatoriSchema.pdr) === pdrAnomali(PdrAnomaloSchema.pdr)
        and midContatoriPrecedente(MidContatoriSchema.annomese) === pdrAnomali(PdrAnomaloSchema.annomese),
      "LEFT_ANTI")
      .join(executionIdPrevDs,executionIdPrevDs(AnnoMeseExecutionIdSchema.annomese_riferimento) === midContatoriPrecedente(PdrAnomaloSchema.annomese),
        "LEFT")
      .select(
        midContatoriPrecedente(MidContatoriSchema.pdr) as MidContatoriSchema.pdr,
        midContatoriPrecedente(MidContatoriSchema.contatore) as MidContatoriSchema.contatore,
        lit(CostantiMid.STATO_INVALIDO) as MidContatoriSchema.stato,
        midContatoriPrecedente(MidContatoriSchema.treatment) as MidContatoriSchema.treatment,
        lit(null) as MidContatoriSchema.data_tracciatura,
        lit(null) as MidContatoriSchema.processo_tracciatura,
        lit(null) as MidContatoriSchema.sessione_tracciatura,
        midContatoriPrecedente(MidContatoriSchema.causale_tracciatura) as MidContatoriSchema.causale_tracciatura,
        lit(null) as MidContatoriSchema.tipo_calcolo,
        lit(null) as MidContatoriSchema.executionid_daily_consumption,
        executionIdPrevDs(AnnoMeseExecutionIdSchema.executionid_max) as MidContatoriSchema.executionid_tracciatura_prev,
        midContatoriPrecedente(MidContatoriSchema.annomese) as MidContatoriSchema.annomese,
        lit(null) as MidContatoriSchema.executionid_tracciatura
      )
      .as[MidContatoriModel]

    pdrAncoraAnomali.union(pdrNonPiuAnomali)
  }

  /** *
   * Flusso SBG
   * Aggiungi inclusioni a mid contatori con stato FORZATO e conteggio impostato
   */
  def aggiungiInclusioni(midContatori: Dataset[MidContatoriModel], inclusioni: Dataset[InclusioniModel], executionIdPrev: java.lang.Long): Dataset[MidContatoriModel] = {
    val inclusioniMid = inclusioni.map(r => MidContatoriModel(
      pdr = r.pdr,
      contatore = r.n,
      stato = CostantiMid.STATO_FORZATO,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = r.annomese
    ))

    midContatori.union(inclusioniMid)
  }

  /** *
   * Flussi AGG (Standard e Back in time)
   * Aggiungi inclusioni a mid contatori con stato FORZATO e conteggio impostato
   */
  def aggiungiInclusioni(midContatori: Dataset[MidContatoriModel], inclusioni: Dataset[InclusioniModel],
                         executionIdPrevDs: Dataset[AnnoMeseExecusionIdModel]): Dataset[MidContatoriModel] = {
    val inclusioniMid = inclusioni.map(r => MidContatoriModel(
      pdr = r.pdr,
      contatore = r.n,
      stato = CostantiMid.STATO_FORZATO,
      executionid_tracciatura_prev = null,
      annomese = r.annomese
    ))

    /** *
     *per executionIdPrevDs so che annomese_riferimento sono valori unici e non ci sono duplicati per
     * quel valore
     */
    val inclusioniMidWithExeIdTracciaturaPrevFixed = inclusioniMid.join(executionIdPrevDs
      , inclusioniMid(MidContatoriSchema.annomese)===executionIdPrevDs(AnnoMeseExecutionIdSchema.annomese_riferimento)
      ,"LEFT")
      .withColumn(MidContatoriSchema.executionid_tracciatura_prev,col(AnnoMeseExecutionIdSchema.executionid_max))
      .selectExpr(MidContatoriSchema.getValues: _*)
      .as[MidContatoriModel]

    midContatori.union(inclusioniMidWithExeIdTracciaturaPrevFixed)
  }

  /** *
   * Flussi AGG (Standard e Back in Time)
   * Aggiungi esclusioni a mid contatori per tracciare che siano stati esclusi volontariamente
   */
  def aggiungiEsclusioni(midContatori: Dataset[MidContatoriModel], esclusi: Dataset[EsclusioniModel],
                         executionIdPrevWithExeIdTracciaturaPrevFixed: Dataset[AnnoMeseExecusionIdModel]): Dataset[MidContatoriModel] = {
    val esclusioniMid = esclusi.map(r => MidContatoriModel(
      pdr = r.pdr,
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      executionid_tracciatura_prev = null,
      annomese = r.annomese
    ))

    /** *
     *per executionIdPrevDs so che annomese_riferimento sono valori unici e non ci sono duplicati per
     * quel valore
     */

    val esclusioniMidWithAnnoMese = esclusioniMid.join(executionIdPrevWithExeIdTracciaturaPrevFixed
      ,esclusioniMid(MidContatoriSchema.annomese) === executionIdPrevWithExeIdTracciaturaPrevFixed(AnnoMeseExecutionIdSchema.annomese_riferimento)
      ,"LEFT")
      .withColumn(MidContatoriSchema.executionid_tracciatura_prev,col(AnnoMeseExecutionIdSchema.executionid_max))
      .selectExpr(MidContatoriSchema.getValues: _*)
      .as[MidContatoriModel]

    midContatori.union(esclusioniMidWithAnnoMese)
  }

  /** *
   * Flussi SBG*
   * Aggiungi esclusioni a mid contatori per tracciare che siano stati esclusi volontariamente
   */
  def aggiungiEsclusioni(midContatori: Dataset[MidContatoriModel], esclusi: Dataset[EsclusioniModel], executionIdPrev: java.lang.Long): Dataset[MidContatoriModel] = {
    val esclusioniMid = esclusi.map(r => MidContatoriModel(
      pdr = r.pdr,
      contatore = 0,
      stato = CostantiMid.STATO_ESCLUSO_FORZATO,
      executionid_tracciatura_prev = executionIdPrev,
      annomese = r.annomese
    ))

    midContatori.union(esclusioniMid)
  }

  /** *
   * Completa i dati mancanti per la tabella MidContatori.
   * Aggiunge il treatment dalla dailyConsumption e i parametri della funzione alla struttura finale della mid
   */
  def
  finalizzaMidContatori(mid: Dataset[MidContatoriModel],
                            dailyConsumption: Dataset[DailyConsumptionModel],
                            dataCalcolo: Date,
                            processoTracciatura: String,
                            sessioneTracciatura: String,
                            tipoCalcolo: String,
                            executionIdDaily: Long,
                            executionId: Long
                           ): Dataset[MidContatoriModel] = {
    // estrae il valore di treatment per pdr
    val pdrTreatment = dailyConsumption
      .where(col(DailyConsumptionSchema.executionid) === executionIdDaily)
      .select(DailyConsumptionSchema.pdr, DailyConsumptionSchema.annomese, DailyConsumptionSchema.treatment)
      .dropDuplicates()

    mid.join(
      pdrTreatment,
      mid(MidContatoriSchema.pdr) === pdrTreatment(DailyConsumptionSchema.pdr)
        and mid(MidContatoriSchema.annomese) === pdrTreatment(DailyConsumptionSchema.annomese),
      "LEFT"
    )
      .select(
        mid(MidContatoriSchema.pdr) as MidContatoriSchema.pdr,
        mid(MidContatoriSchema.contatore) as MidContatoriSchema.contatore,
        mid(MidContatoriSchema.stato) as MidContatoriSchema.stato,
        coalesce(pdrTreatment(MidContatoriSchema.treatment), mid(MidContatoriSchema.treatment)) as MidContatoriSchema.treatment,
        lit(dataCalcolo) as MidContatoriSchema.data_tracciatura,
        lit(processoTracciatura) as MidContatoriSchema.processo_tracciatura,
        lit(sessioneTracciatura) as MidContatoriSchema.sessione_tracciatura,
        mid(MidContatoriSchema.causale_tracciatura) as MidContatoriSchema.causale_tracciatura,
        lit(tipoCalcolo) as MidContatoriSchema.tipo_calcolo,
        lit(executionIdDaily) as MidContatoriSchema.executionid_daily_consumption,
        mid(MidContatoriSchema.executionid_tracciatura_prev) as MidContatoriSchema.executionid_tracciatura_prev,
        mid(MidContatoriSchema.annomese) as MidContatoriSchema.annomese,
        lit(executionId) as MidContatoriSchema.executionid_tracciatura
      )
      .selectExpr(MidContatoriSchema.getValues: _*)
      .as[MidContatoriModel]
  }

}
