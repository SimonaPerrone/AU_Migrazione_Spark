package it.eng.au.ERP.trasformations.INT

import it.eng.au.ERP.model.rcu.{RcuAziendaPModel, RcuPodInterconnesionePModel}
import it.eng.au.ERP.schema.au.flussoMisureInterconnessioneSchema
import it.eng.au.ERP.schema.erp._
import it.eng.au.ERP.schema.rcu.{RcuAziendaPSchema, RcuPodInterconnesionePSchema}
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.setting.PropertyUtility
import it.eng.au.ERP.utility.functions.Constants
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.log4j.Logger

object CalcoloPrelevatoPuntiPrelievoOrariINTTrasformation {

  val rank = "rank"
  val casoPIVA = "casoPIVA"
  @transient private lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def calcoloErpValidatedInt(
                              dsFlussoMisureInterconnessione: DataFrame,
                              dsRcuPodInterconnesioneP: Dataset[RcuPodInterconnesionePModel],
                              args: ERPArgsConfig,
                              currentPartitions: Int,
                              podExcluded: List[String],
                              timestamp: Long,
                              year: Option[Int],
                              month: Option[Int],
                              area: Option[String],
                              singola_piva_distributore: Option[String]
                            )
                            (implicit spark: SparkSession): DataFrame = {


    //note
    //currentPartitions can be avoided and decide to tspark the number of partition based
    //on cluster cores
    //    val currentPartitions = dsFlussoMisureInterconnessione.rdd.getNumPartitions

    val windowsSpec: WindowSpec = Window.partitionBy(flussoMisureInterconnessioneSchema.pod, erpValidatedIntSchema.giorno)
      .orderBy(col(flussoMisureInterconnessioneSchema.time_stamp).desc_nulls_last)


    val dsFlussoMisureInterconnessioneFiltered =
      CalcoloPrelevatoPuntiPrelievoOrariINT.dfErpValidatoIntFilteredAnnoMese(dsFlussoMisureInterconnessione, year, month)
        //      .repartition(currentPartitions,col(flussoMisureInterconnessioneSchema.pod))
        //      .filter(col(flussoMisureInterconnessioneSchema.anno).cast(IntegerType) === year)
        //      .filter(col(flussoMisureInterconnessioneSchema.mese).cast(IntegerType) === month)
        .filter(col(flussoMisureInterconnessioneSchema.tipo_dest) === Constants.tipo_dest)
        .filter(col(flussoMisureInterconnessioneSchema.validato) === Constants.validato_S)
        //      .filter(!(col(vAggregazioniMisureQuartorarieSchema.pod).isin(podExcluded:_*)))
        .withColumn(erpValidatedIntSchema.giorno, col(flussoMisureInterconnessioneSchema.giorno))

    // DEBUG: log counts along the pipeline to diagnose empty writes
    try {
      val cFiltered = dsFlussoMisureInterconnessioneFiltered.count()
      logger.info(s"[INT-VALIDATED-DEBUG] After AU filters -> count=$cFiltered, year=${year.getOrElse(-1)}, month=${month.getOrElse(-1)}")
    } catch { case _: Throwable => logger.warn("[INT-VALIDATED-DEBUG] Count failed on dsFlussoMisureInterconnessioneFiltered") }

    val dsFlussoMisureInterconnessioneFilteredTimestampMax = dsFlussoMisureInterconnessioneFiltered
      .withColumn(rank,
        row_number().over(windowsSpec).as(rank))
      .filter(col(rank) === 1)

    // Lookup area from RCU view as per Enermia spec (RCU.RCU_POD_INTERCONNESSIONE_P)
    val dfrcuPodP = dsRcuPodInterconnesioneP
      .select(
        col(RcuPodInterconnesionePSchema.cod_pod_int),
        col(RcuPodInterconnesionePSchema.t_area_rif)
      )

    val leftFlusso  = dsFlussoMisureInterconnessioneFilteredTimestampMax.alias("l")
    val rightRcuPod = dfrcuPodP.alias("r")

    val dsFlussoMisureInterconnessioneJoinRcuPodP = leftFlusso
      .join(
        rightRcuPod,
        trim(col(s"l.${flussoMisureInterconnessioneSchema.pod.toString}")) ===
          trim(col(s"r.${RcuPodInterconnesionePSchema.cod_pod_int.toString}")),
        "left"
      )

    // DEBUG: post-join stats and sample
    try {
      val cTsMax = dsFlussoMisureInterconnessioneFilteredTimestampMax.count()
      val cJoin = dsFlussoMisureInterconnessioneJoinRcuPodP.count()
      val cAreaNotNull = dsFlussoMisureInterconnessioneJoinRcuPodP
        .filter(col(s"r.${RcuPodInterconnesionePSchema.t_area_rif.toString}").isNotNull).count()
      logger.info(s"[INT-VALIDATED-DEBUG] After tsMax -> count=$cTsMax; After join RCU -> count=$cJoin; area_not_null=$cAreaNotNull")
      val sampleNull = dsFlussoMisureInterconnessioneJoinRcuPodP
        .filter(col(s"r.${RcuPodInterconnesionePSchema.t_area_rif.toString}").isNull)
        .select(col(s"l.${flussoMisureInterconnessioneSchema.pod.toString}").alias("pod"))
        .limit(10)
        .toJSON
        .collect()
        .mkString(",")
      if (sampleNull.nonEmpty) logger.info(s"[INT-VALIDATED-DEBUG] Sample POD with NULL area (first 10): ${sampleNull}")
    } catch { case _: Throwable => logger.warn("[INT-VALIDATED-DEBUG] Post-join stats failed") }

    val dfFlussoMisureInterconnessioneJoined =
      CalcoloPrelevatoPuntiPrelievoOrariINT.dfFlussoMisureInterconnessioneFilteredArea(
        dsFlussoMisureInterconnessioneJoinRcuPodP,
        area
      )
        // map area explicitly from the joined column (unqualified name present after the join)
        .withColumn(erpValidatedIntSchema.area, col(RcuPodInterconnesionePSchema.t_area_rif))
        .withColumn(erpValidatedIntSchema.piva_distr_dest, col(flussoMisureInterconnessioneSchema.pivagdrmis))
        .withColumn(erpValidatedIntSchema.executionid, lit(timestamp))

    val finalDf = dfFlussoMisureInterconnessioneJoined.selectExpr(erpValidatedIntSchema.getValues: _*)

    finalDf
  }

  def calcoloErpDettaglioInt(
                              dfErpValidatedInt: DataFrame,
                              dsRcuAziendaP: Dataset[RcuAziendaPModel],
                              args: ERPArgsConfig,
                              singola_piva_distributore: Option[String]
                            )
                            (implicit spark: SparkSession): DataFrame = {

    val dsRcuAziendaPFiltered = dsRcuAziendaP
      .withColumnRenamed(RcuAziendaPSchema.t_rag_soc, erpAggregatoOSchema.rag_soc_distr)
      .select(
        col(RcuAziendaPSchema.t_piva),
        col(erpAggregatoOSchema.rag_soc_distr)
      )

    val dfErpValidatedIntFiltered = CalcoloPrelevatoPuntiPrelievoOrariINT
      .dfErpValidatoIntFilteredPiva(dfErpValidatedInt, singola_piva_distributore)

    val dfErpValidatedIntEnriched = dfErpValidatedIntFiltered.
      withColumn(casoPIVA
        , CalcoloPrelevatoPuntiPrelievoOrariINT
          .funzionePivaCasi(
            col(erpValidatedIntSchema.pivagdrmis),
            col(erpValidatedIntSchema.pivagdrinst),
            col(erpValidatedIntSchema.pivagdralt))
      )

    // Apply Ka and loss coefficients to energy curves as per Enermia spec
    val dfScaledEnergy =
      CalcoloPrelevatoPuntiPrelievoOrariINT
        .dfCurvaImmissione(
          CalcoloPrelevatoPuntiPrelievoOrariINT.dfCurvaPrelievo(dfErpValidatedIntEnriched)
        )

    // Build difference datasets (prelievo - immissione) and (immissione - prelievo)
    val dfPrel = CalcoloPrelevatoPuntiPrelievoOrariINT.dfCampioneDiff1(dfScaledEnergy)
      .persist()
    val dfImm = CalcoloPrelevatoPuntiPrelievoOrariINT.dfCampioneDiff2(dfScaledEnergy)
      .persist()

    val dfCase1Diff1 = dfPrel.filter(col(casoPIVA) === 1)
    val dfCase1Diff2 = dfImm.filter(col(casoPIVA) === 1)

    val dfCase2Diff1 = dfPrel.filter(col(casoPIVA) === 2)
    val dfCase2Diff2 = dfImm.filter(col(casoPIVA) === 2)

    val dfCase3Diff1 = dfPrel.filter(col(casoPIVA) === 3)
    val dfCase3Diff2 = dfImm.filter(col(casoPIVA) === 3)

    val dfCase4Diff1 = dfPrel.filter(col(casoPIVA) === 4)
    val dfCase4Diff2 = dfImm.filter(col(casoPIVA) === 4)

    val dfCase5Diff2 = dfImm.filter(col(casoPIVA) === 5)

    //case1
    val dfCase1Enriched1 =
      dfCase1Diff2.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrmis))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    val dfCase1Enriched2 =
      dfCase1Diff1.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrinst))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    val dfCase1Enriched3 =
      dfCase1Diff1.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdralt))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    //case2
    val dfCase2Enriched1 =
      dfCase2Diff2.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrmis))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    val dfCase2Enriched2 =
      dfCase2Diff1.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdralt))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    //case3
    val dfCase3Enriched1 =
      dfCase3Diff2.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrmis))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    val dfCase3Enriched2 =
      dfCase3Diff1.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrinst))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    //case4
    val dfCase4Enriched1 =
      dfCase4Diff2.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrmis))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    val dfCase4Enriched2 =
      dfCase4Diff1.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrinst))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    //case5
    val dfCase5Enriched2 =
      dfCase5Diff2.withColumn(erpDettaglioINTSchema.piva_distr, col(erpValidatedIntSchema.pivagdrmis))
        .selectExpr(erpDettaglioINTexceptRagSocDistrSchema.getValues: _*)

    val dfUnionCase = dfCase1Enriched1
      .unionByName(dfCase1Enriched2)
      .unionByName(dfCase1Enriched3)
      .unionByName(dfCase2Enriched1)
      .unionByName(dfCase2Enriched2)
      .unionByName(dfCase3Enriched1)
      .unionByName(dfCase3Enriched2)
      .unionByName(dfCase4Enriched1)
      .unionByName(dfCase4Enriched2)
      .unionByName(dfCase5Enriched2)

    val dfUnionCaseJoined = dfUnionCase.join(
      broadcast(dsRcuAziendaPFiltered)
      , col(erpDettaglioINTSchema.piva_distr) === col(RcuAziendaPSchema.t_piva),
      "left"
    )
      .selectExpr(erpDettaglioINTSchema.getValues: _*)

    dfPrel.unpersist()
    dfImm.unpersist()

    dfUnionCaseJoined
  }

  def calcoloErpAggregatoInt(dfErpDettaglioInt: DataFrame,
                             timestamp: Long
                            )
                            (implicit spark: SparkSession): DataFrame = {


    val dfAggregqqtion = dfErpDettaglioInt.groupBy(
      col(erpDettaglioINTSchema.anno),
      col(erpDettaglioINTSchema.mese),
      col(erpDettaglioINTSchema.giorno),
      col(erpDettaglioINTSchema.area),
      col(erpDettaglioINTSchema.piva_distr),
      col(erpDettaglioINTSchema.rag_soc_distr)
    ).agg(
      sum(col(erpDettaglioINTSchema.q1)).alias(erpAggregatoINTSchema.q1),
      sum(col(erpDettaglioINTSchema.q2)).alias(erpAggregatoINTSchema.q2),
      sum(col(erpDettaglioINTSchema.q3)).alias(erpAggregatoINTSchema.q3),
      sum(col(erpDettaglioINTSchema.q4)).alias(erpAggregatoINTSchema.q4),
      sum(col(erpDettaglioINTSchema.q5)).alias(erpAggregatoINTSchema.q5),
      sum(col(erpDettaglioINTSchema.q6)).alias(erpAggregatoINTSchema.q6),
      sum(col(erpDettaglioINTSchema.q7)).alias(erpAggregatoINTSchema.q7),
      sum(col(erpDettaglioINTSchema.q8)).alias(erpAggregatoINTSchema.q8),
      sum(col(erpDettaglioINTSchema.q9)).alias(erpAggregatoINTSchema.q9),
      sum(col(erpDettaglioINTSchema.q10)).alias(erpAggregatoINTSchema.q10),
      sum(col(erpDettaglioINTSchema.q11)).alias(erpAggregatoINTSchema.q11),
      sum(col(erpDettaglioINTSchema.q12)).alias(erpAggregatoINTSchema.q12),
      sum(col(erpDettaglioINTSchema.q13)).alias(erpAggregatoINTSchema.q13),
      sum(col(erpDettaglioINTSchema.q14)).alias(erpAggregatoINTSchema.q14),
      sum(col(erpDettaglioINTSchema.q15)).alias(erpAggregatoINTSchema.q15),
      sum(col(erpDettaglioINTSchema.q16)).alias(erpAggregatoINTSchema.q16),
      sum(col(erpDettaglioINTSchema.q17)).alias(erpAggregatoINTSchema.q17),
      sum(col(erpDettaglioINTSchema.q18)).alias(erpAggregatoINTSchema.q18),
      sum(col(erpDettaglioINTSchema.q19)).alias(erpAggregatoINTSchema.q19),
      sum(col(erpDettaglioINTSchema.q20)).alias(erpAggregatoINTSchema.q20),
      sum(col(erpDettaglioINTSchema.q21)).alias(erpAggregatoINTSchema.q21),
      sum(col(erpDettaglioINTSchema.q22)).alias(erpAggregatoINTSchema.q22),
      sum(col(erpDettaglioINTSchema.q23)).alias(erpAggregatoINTSchema.q23),
      sum(col(erpDettaglioINTSchema.q24)).alias(erpAggregatoINTSchema.q24),
      sum(col(erpDettaglioINTSchema.q25)).alias(erpAggregatoINTSchema.q25),
      sum(col(erpDettaglioINTSchema.q26)).alias(erpAggregatoINTSchema.q26),
      sum(col(erpDettaglioINTSchema.q27)).alias(erpAggregatoINTSchema.q27),
      sum(col(erpDettaglioINTSchema.q28)).alias(erpAggregatoINTSchema.q28),
      sum(col(erpDettaglioINTSchema.q29)).alias(erpAggregatoINTSchema.q29),
      sum(col(erpDettaglioINTSchema.q30)).alias(erpAggregatoINTSchema.q30),
      sum(col(erpDettaglioINTSchema.q31)).alias(erpAggregatoINTSchema.q31),
      sum(col(erpDettaglioINTSchema.q32)).alias(erpAggregatoINTSchema.q32),
      sum(col(erpDettaglioINTSchema.q33)).alias(erpAggregatoINTSchema.q33),
      sum(col(erpDettaglioINTSchema.q34)).alias(erpAggregatoINTSchema.q34),
      sum(col(erpDettaglioINTSchema.q35)).alias(erpAggregatoINTSchema.q35),
      sum(col(erpDettaglioINTSchema.q36)).alias(erpAggregatoINTSchema.q36),
      sum(col(erpDettaglioINTSchema.q37)).alias(erpAggregatoINTSchema.q37),
      sum(col(erpDettaglioINTSchema.q38)).alias(erpAggregatoINTSchema.q38),
      sum(col(erpDettaglioINTSchema.q39)).alias(erpAggregatoINTSchema.q39),
      sum(col(erpDettaglioINTSchema.q40)).alias(erpAggregatoINTSchema.q40),
      sum(col(erpDettaglioINTSchema.q41)).alias(erpAggregatoINTSchema.q41),
      sum(col(erpDettaglioINTSchema.q42)).alias(erpAggregatoINTSchema.q42),
      sum(col(erpDettaglioINTSchema.q43)).alias(erpAggregatoINTSchema.q43),
      sum(col(erpDettaglioINTSchema.q44)).alias(erpAggregatoINTSchema.q44),
      sum(col(erpDettaglioINTSchema.q45)).alias(erpAggregatoINTSchema.q45),
      sum(col(erpDettaglioINTSchema.q46)).alias(erpAggregatoINTSchema.q46),
      sum(col(erpDettaglioINTSchema.q47)).alias(erpAggregatoINTSchema.q47),
      sum(col(erpDettaglioINTSchema.q48)).alias(erpAggregatoINTSchema.q48),
      sum(col(erpDettaglioINTSchema.q49)).alias(erpAggregatoINTSchema.q49),
      sum(col(erpDettaglioINTSchema.q50)).alias(erpAggregatoINTSchema.q50),
      sum(col(erpDettaglioINTSchema.q51)).alias(erpAggregatoINTSchema.q51),
      sum(col(erpDettaglioINTSchema.q52)).alias(erpAggregatoINTSchema.q52),
      sum(col(erpDettaglioINTSchema.q53)).alias(erpAggregatoINTSchema.q53),
      sum(col(erpDettaglioINTSchema.q54)).alias(erpAggregatoINTSchema.q54),
      sum(col(erpDettaglioINTSchema.q55)).alias(erpAggregatoINTSchema.q55),
      sum(col(erpDettaglioINTSchema.q56)).alias(erpAggregatoINTSchema.q56),
      sum(col(erpDettaglioINTSchema.q57)).alias(erpAggregatoINTSchema.q57),
      sum(col(erpDettaglioINTSchema.q58)).alias(erpAggregatoINTSchema.q58),
      sum(col(erpDettaglioINTSchema.q59)).alias(erpAggregatoINTSchema.q59),
      sum(col(erpDettaglioINTSchema.q60)).alias(erpAggregatoINTSchema.q60),
      sum(col(erpDettaglioINTSchema.q61)).alias(erpAggregatoINTSchema.q61),
      sum(col(erpDettaglioINTSchema.q62)).alias(erpAggregatoINTSchema.q62),
      sum(col(erpDettaglioINTSchema.q63)).alias(erpAggregatoINTSchema.q63),
      sum(col(erpDettaglioINTSchema.q64)).alias(erpAggregatoINTSchema.q64),
      sum(col(erpDettaglioINTSchema.q65)).alias(erpAggregatoINTSchema.q65),
      sum(col(erpDettaglioINTSchema.q66)).alias(erpAggregatoINTSchema.q66),
      sum(col(erpDettaglioINTSchema.q67)).alias(erpAggregatoINTSchema.q67),
      sum(col(erpDettaglioINTSchema.q68)).alias(erpAggregatoINTSchema.q68),
      sum(col(erpDettaglioINTSchema.q69)).alias(erpAggregatoINTSchema.q69),
      sum(col(erpDettaglioINTSchema.q70)).alias(erpAggregatoINTSchema.q70),
      sum(col(erpDettaglioINTSchema.q71)).alias(erpAggregatoINTSchema.q71),
      sum(col(erpDettaglioINTSchema.q72)).alias(erpAggregatoINTSchema.q72),
      sum(col(erpDettaglioINTSchema.q73)).alias(erpAggregatoINTSchema.q73),
      sum(col(erpDettaglioINTSchema.q74)).alias(erpAggregatoINTSchema.q74),
      sum(col(erpDettaglioINTSchema.q75)).alias(erpAggregatoINTSchema.q75),
      sum(col(erpDettaglioINTSchema.q76)).alias(erpAggregatoINTSchema.q76),
      sum(col(erpDettaglioINTSchema.q77)).alias(erpAggregatoINTSchema.q77),
      sum(col(erpDettaglioINTSchema.q78)).alias(erpAggregatoINTSchema.q78),
      sum(col(erpDettaglioINTSchema.q79)).alias(erpAggregatoINTSchema.q79),
      sum(col(erpDettaglioINTSchema.q80)).alias(erpAggregatoINTSchema.q80),
      sum(col(erpDettaglioINTSchema.q81)).alias(erpAggregatoINTSchema.q81),
      sum(col(erpDettaglioINTSchema.q82)).alias(erpAggregatoINTSchema.q82),
      sum(col(erpDettaglioINTSchema.q83)).alias(erpAggregatoINTSchema.q83),
      sum(col(erpDettaglioINTSchema.q84)).alias(erpAggregatoINTSchema.q84),
      sum(col(erpDettaglioINTSchema.q85)).alias(erpAggregatoINTSchema.q85),
      sum(col(erpDettaglioINTSchema.q86)).alias(erpAggregatoINTSchema.q86),
      sum(col(erpDettaglioINTSchema.q87)).alias(erpAggregatoINTSchema.q87),
      sum(col(erpDettaglioINTSchema.q88)).alias(erpAggregatoINTSchema.q88),
      sum(col(erpDettaglioINTSchema.q89)).alias(erpAggregatoINTSchema.q89),
      sum(col(erpDettaglioINTSchema.q90)).alias(erpAggregatoINTSchema.q90),
      sum(col(erpDettaglioINTSchema.q91)).alias(erpAggregatoINTSchema.q91),
      sum(col(erpDettaglioINTSchema.q92)).alias(erpAggregatoINTSchema.q92),
      sum(col(erpDettaglioINTSchema.q93)).alias(erpAggregatoINTSchema.q93),
      sum(col(erpDettaglioINTSchema.q94)).alias(erpAggregatoINTSchema.q94),
      sum(col(erpDettaglioINTSchema.q95)).alias(erpAggregatoINTSchema.q95),
      sum(col(erpDettaglioINTSchema.q96)).alias(erpAggregatoINTSchema.q96),
      sum(col(erpDettaglioINTSchema.q97)).alias(erpAggregatoINTSchema.q97),
      sum(col(erpDettaglioINTSchema.q98)).alias(erpAggregatoINTSchema.q98),
      sum(col(erpDettaglioINTSchema.q99)).alias(erpAggregatoINTSchema.q99),
      sum(col(erpDettaglioINTSchema.q100)).alias(erpAggregatoINTSchema.q100)
    )
      .withColumn(erpAggregatoINTSchema.executionid, lit(timestamp))
      // Build ISO giorno as new column to avoid name-shadowing, then replace
      .withColumn("__anno_str__", col(erpDettaglioINTSchema.anno).cast("string"))
      .withColumn("__mese_str__", lpad(col(erpDettaglioINTSchema.mese).cast("string"), 2, "0"))
      .withColumn("__giorno_str__", lpad(col(erpDettaglioINTSchema.giorno).cast("string"), 2, "0"))
      .withColumn("__giorno_iso__", concat_ws("-", col("__anno_str__"), col("__mese_str__"), col("__giorno_str__")))
      .drop(erpAggregatoINTSchema.giorno)
      .withColumnRenamed("__giorno_iso__", erpAggregatoINTSchema.giorno)
      .withColumn(erpAggregatoINTSchema.anno, col("__anno_str__"))
      .withColumn(erpAggregatoINTSchema.mese, col("__mese_str__"))
      .drop("__anno_str__", "__mese_str__", "__giorno_str__")

    val finalDf = dfAggregqqtion.selectExpr(erpAggregatoINTSchema.getValues: _*)

    finalDf
  }

}
