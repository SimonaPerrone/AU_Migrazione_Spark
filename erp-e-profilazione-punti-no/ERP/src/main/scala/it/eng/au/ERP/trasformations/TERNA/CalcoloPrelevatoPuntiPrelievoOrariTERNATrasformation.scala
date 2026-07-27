package it.eng.au.ERP.trasformations.TERNA

import it.eng.au.ERP.model.au.podConnessioneModel
import it.eng.au.ERP.model.rcu.RcuAziendaPModel
import it.eng.au.ERP.schema.au.{aggregazioniMisureQuartorarieSchema, podConnessioneSchema}
import it.eng.au.ERP.schema.erp.erpAggregatoOSchema
import it.eng.au.ERP.schema.rcu.RcuAziendaPSchema
import it.eng.au.ERP.trasformations.CalcoloPrelevatoPuntiPrelievoOrari
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.functions.Constants
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{broadcast, col, lit, sum, lpad, concat_ws}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object CalcoloPrelevatoPuntiPrelievoOrariTERNATrasformation {

  @transient private lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def calcoloPrelevatoPuntiPrelievoOrari(dfAggregazioniMisureQuartorarie: DataFrame,
                                         dsRcuAziendaP: Dataset[RcuAziendaPModel],
                                         dsPodConnessione: Dataset[podConnessioneModel],
                                         args: ERPArgsConfig,
                                         currentPartitions: Int,
                                         podExcluded: List[String],
                                         timestamp: Long,
                                         terna: Boolean,
                                         year: Option[Int],
                                         month: Option[Int],
                                         area: Option[String],
                                         singola_piva_distributore: Option[String]
                                        )
                                        (implicit spark: SparkSession): DataFrame = {


    val dsFlussoMisureInterconnessioneFilteredAnnoMese = dfAggregazioniMisureQuartorarie
    //      .repartition(currentPartitions,col(aggregazioniMisureQuartorarieSchema.pod))
    //      .filter(col(aggregazioniMisureQuartorarieSchema.anno) === year)
    //      .filter(col(aggregazioniMisureQuartorarieSchema.mese) === month)
    //      .filter(!(col(aggregazioniMisureQuartorarieSchema.pod).isin(podExcluded:_*)))

    val dsFlussoMisureInterconnessioneFilteredGhigliottina = args.ghigliottina match {
      case Some(g) =>
        logger.info(s"Applicazione filtro ghigliottina su TERNA: ANNOMESEGIORNO_DIR <= $g")
        dsFlussoMisureInterconnessioneFilteredAnnoMese.filter(
          col(aggregazioniMisureQuartorarieSchema.annomesegiornodir) <= lit(g)
        )
      case None => dsFlussoMisureInterconnessioneFilteredAnnoMese
    }

    val dsFlussoMisureInterconnessioneFiltered = CalcoloPrelevatoPuntiPrelievoOrari
      .aggregazioniMisureQuartorarieFiltered(dsFlussoMisureInterconnessioneFilteredGhigliottina,
        year, month, area, singola_piva_distributore)

    // Perimetro corretto TERNA (usa t_connessione <> 'N')
    val dsPodConnessioneFiltered = CalcoloPrelevatoPuntiPrelievoOrari
      .podConnessioneDataExtraction(dsPodConnessione, terna = true, currentPartitions)

    val dsRcuAziendaPFiltered = dsRcuAziendaP
      .withColumnRenamed(RcuAziendaPSchema.t_piva, erpAggregatoOSchema.piva_distr)
      .withColumnRenamed(RcuAziendaPSchema.t_rag_soc, erpAggregatoOSchema.rag_soc_distr)
      .select(
        col(RcuAziendaPSchema.n_id_azienda),
        col(erpAggregatoOSchema.piva_distr),
        col(erpAggregatoOSchema.rag_soc_distr)
      )

    val dfJoin1 = dsFlussoMisureInterconnessioneFiltered
      .join(dsPodConnessioneFiltered,
        col(aggregazioniMisureQuartorarieSchema.pod) === col(podConnessioneSchema.pod14)
        ,
        "inner"
      )

    val dfJoin2 = dfJoin1.join(
      broadcast(dsRcuAziendaPFiltered)
      , col(aggregazioniMisureQuartorarieSchema.n_id_distr) === col(RcuAziendaPSchema.n_id_azienda),
      "left"
    )

    val dfAggregqqtion = dfJoin2.groupBy(
      col(aggregazioniMisureQuartorarieSchema.anno),
      col(aggregazioniMisureQuartorarieSchema.mese),
      col(aggregazioniMisureQuartorarieSchema.giorno),
      col(aggregazioniMisureQuartorarieSchema.area),
      col(erpAggregatoOSchema.piva_distr),
      col(erpAggregatoOSchema.rag_soc_distr)
    ).agg(
      sum(col(aggregazioniMisureQuartorarieSchema.h1_q1)).alias(erpAggregatoOSchema.q1),
      sum(col(aggregazioniMisureQuartorarieSchema.h1_q2)).alias(erpAggregatoOSchema.q2),
      sum(col(aggregazioniMisureQuartorarieSchema.h1_q3)).alias(erpAggregatoOSchema.q3),
      sum(col(aggregazioniMisureQuartorarieSchema.h1_q4)).alias(erpAggregatoOSchema.q4),
      sum(col(aggregazioniMisureQuartorarieSchema.h2_q5)).alias(erpAggregatoOSchema.q5),
      sum(col(aggregazioniMisureQuartorarieSchema.h2_q6)).alias(erpAggregatoOSchema.q6),
      sum(col(aggregazioniMisureQuartorarieSchema.h2_q7)).alias(erpAggregatoOSchema.q7),
      sum(col(aggregazioniMisureQuartorarieSchema.h2_q8)).alias(erpAggregatoOSchema.q8),
      sum(col(aggregazioniMisureQuartorarieSchema.h3_q9)).alias(erpAggregatoOSchema.q9),
      sum(col(aggregazioniMisureQuartorarieSchema.h3_q10)).alias(erpAggregatoOSchema.q10),
      sum(col(aggregazioniMisureQuartorarieSchema.h3_q11)).alias(erpAggregatoOSchema.q11),
      sum(col(aggregazioniMisureQuartorarieSchema.h3_q12)).alias(erpAggregatoOSchema.q12),
      sum(col(aggregazioniMisureQuartorarieSchema.h4_q13)).alias(erpAggregatoOSchema.q13),
      sum(col(aggregazioniMisureQuartorarieSchema.h4_q14)).alias(erpAggregatoOSchema.q14),
      sum(col(aggregazioniMisureQuartorarieSchema.h4_q15)).alias(erpAggregatoOSchema.q15),
      sum(col(aggregazioniMisureQuartorarieSchema.h4_q16)).alias(erpAggregatoOSchema.q16),
      sum(col(aggregazioniMisureQuartorarieSchema.h5_q17)).alias(erpAggregatoOSchema.q17),
      sum(col(aggregazioniMisureQuartorarieSchema.h5_q18)).alias(erpAggregatoOSchema.q18),
      sum(col(aggregazioniMisureQuartorarieSchema.h5_q19)).alias(erpAggregatoOSchema.q19),
      sum(col(aggregazioniMisureQuartorarieSchema.h5_q20)).alias(erpAggregatoOSchema.q20),
      sum(col(aggregazioniMisureQuartorarieSchema.h6_q21)).alias(erpAggregatoOSchema.q21),
      sum(col(aggregazioniMisureQuartorarieSchema.h6_q22)).alias(erpAggregatoOSchema.q22),
      sum(col(aggregazioniMisureQuartorarieSchema.h6_q23)).alias(erpAggregatoOSchema.q23),
      sum(col(aggregazioniMisureQuartorarieSchema.h6_q24)).alias(erpAggregatoOSchema.q24),
      sum(col(aggregazioniMisureQuartorarieSchema.h7_q25)).alias(erpAggregatoOSchema.q25),
      sum(col(aggregazioniMisureQuartorarieSchema.h7_q26)).alias(erpAggregatoOSchema.q26),
      sum(col(aggregazioniMisureQuartorarieSchema.h7_q27)).alias(erpAggregatoOSchema.q27),
      sum(col(aggregazioniMisureQuartorarieSchema.h7_q28)).alias(erpAggregatoOSchema.q28),
      sum(col(aggregazioniMisureQuartorarieSchema.h8_q29)).alias(erpAggregatoOSchema.q29),
      sum(col(aggregazioniMisureQuartorarieSchema.h8_q30)).alias(erpAggregatoOSchema.q30),
      sum(col(aggregazioniMisureQuartorarieSchema.h8_q31)).alias(erpAggregatoOSchema.q31),
      sum(col(aggregazioniMisureQuartorarieSchema.h8_q32)).alias(erpAggregatoOSchema.q32),
      sum(col(aggregazioniMisureQuartorarieSchema.h9_q33)).alias(erpAggregatoOSchema.q33),
      sum(col(aggregazioniMisureQuartorarieSchema.h9_q34)).alias(erpAggregatoOSchema.q34),
      sum(col(aggregazioniMisureQuartorarieSchema.h9_q35)).alias(erpAggregatoOSchema.q35),
      sum(col(aggregazioniMisureQuartorarieSchema.h9_q36)).alias(erpAggregatoOSchema.q36),
      sum(col(aggregazioniMisureQuartorarieSchema.h10_q37)).alias(erpAggregatoOSchema.q37),
      sum(col(aggregazioniMisureQuartorarieSchema.h10_q38)).alias(erpAggregatoOSchema.q38),
      sum(col(aggregazioniMisureQuartorarieSchema.h10_q39)).alias(erpAggregatoOSchema.q39),
      sum(col(aggregazioniMisureQuartorarieSchema.h10_q40)).alias(erpAggregatoOSchema.q40),
      sum(col(aggregazioniMisureQuartorarieSchema.h11_q41)).alias(erpAggregatoOSchema.q41),
      sum(col(aggregazioniMisureQuartorarieSchema.h11_q42)).alias(erpAggregatoOSchema.q42),
      sum(col(aggregazioniMisureQuartorarieSchema.h11_q43)).alias(erpAggregatoOSchema.q43),
      sum(col(aggregazioniMisureQuartorarieSchema.h11_q44)).alias(erpAggregatoOSchema.q44),
      sum(col(aggregazioniMisureQuartorarieSchema.h12_q45)).alias(erpAggregatoOSchema.q45),
      sum(col(aggregazioniMisureQuartorarieSchema.h12_q46)).alias(erpAggregatoOSchema.q46),
      sum(col(aggregazioniMisureQuartorarieSchema.h12_q47)).alias(erpAggregatoOSchema.q47),
      sum(col(aggregazioniMisureQuartorarieSchema.h12_q48)).alias(erpAggregatoOSchema.q48),
      sum(col(aggregazioniMisureQuartorarieSchema.h13_q49)).alias(erpAggregatoOSchema.q49),
      sum(col(aggregazioniMisureQuartorarieSchema.h13_q50)).alias(erpAggregatoOSchema.q50),
      sum(col(aggregazioniMisureQuartorarieSchema.h13_q51)).alias(erpAggregatoOSchema.q51),
      sum(col(aggregazioniMisureQuartorarieSchema.h13_q52)).alias(erpAggregatoOSchema.q52),
      sum(col(aggregazioniMisureQuartorarieSchema.h14_q53)).alias(erpAggregatoOSchema.q53),
      sum(col(aggregazioniMisureQuartorarieSchema.h14_q54)).alias(erpAggregatoOSchema.q54),
      sum(col(aggregazioniMisureQuartorarieSchema.h14_q55)).alias(erpAggregatoOSchema.q55),
      sum(col(aggregazioniMisureQuartorarieSchema.h14_q56)).alias(erpAggregatoOSchema.q56),
      sum(col(aggregazioniMisureQuartorarieSchema.h15_q57)).alias(erpAggregatoOSchema.q57),
      sum(col(aggregazioniMisureQuartorarieSchema.h15_q58)).alias(erpAggregatoOSchema.q58),
      sum(col(aggregazioniMisureQuartorarieSchema.h15_q59)).alias(erpAggregatoOSchema.q59),
      sum(col(aggregazioniMisureQuartorarieSchema.h15_q60)).alias(erpAggregatoOSchema.q60),
      sum(col(aggregazioniMisureQuartorarieSchema.h16_q61)).alias(erpAggregatoOSchema.q61),
      sum(col(aggregazioniMisureQuartorarieSchema.h16_q62)).alias(erpAggregatoOSchema.q62),
      sum(col(aggregazioniMisureQuartorarieSchema.h16_q63)).alias(erpAggregatoOSchema.q63),
      sum(col(aggregazioniMisureQuartorarieSchema.h16_q64)).alias(erpAggregatoOSchema.q64),
      sum(col(aggregazioniMisureQuartorarieSchema.h17_q65)).alias(erpAggregatoOSchema.q65),
      sum(col(aggregazioniMisureQuartorarieSchema.h17_q66)).alias(erpAggregatoOSchema.q66),
      sum(col(aggregazioniMisureQuartorarieSchema.h17_q67)).alias(erpAggregatoOSchema.q67),
      sum(col(aggregazioniMisureQuartorarieSchema.h17_q68)).alias(erpAggregatoOSchema.q68),
      sum(col(aggregazioniMisureQuartorarieSchema.h18_q69)).alias(erpAggregatoOSchema.q69),
      sum(col(aggregazioniMisureQuartorarieSchema.h18_q70)).alias(erpAggregatoOSchema.q70),
      sum(col(aggregazioniMisureQuartorarieSchema.h18_q71)).alias(erpAggregatoOSchema.q71),
      sum(col(aggregazioniMisureQuartorarieSchema.h18_q72)).alias(erpAggregatoOSchema.q72),
      sum(col(aggregazioniMisureQuartorarieSchema.h19_q73)).alias(erpAggregatoOSchema.q73),
      sum(col(aggregazioniMisureQuartorarieSchema.h19_q74)).alias(erpAggregatoOSchema.q74),
      sum(col(aggregazioniMisureQuartorarieSchema.h19_q75)).alias(erpAggregatoOSchema.q75),
      sum(col(aggregazioniMisureQuartorarieSchema.h19_q76)).alias(erpAggregatoOSchema.q76),
      sum(col(aggregazioniMisureQuartorarieSchema.h20_q77)).alias(erpAggregatoOSchema.q77),
      sum(col(aggregazioniMisureQuartorarieSchema.h20_q78)).alias(erpAggregatoOSchema.q78),
      sum(col(aggregazioniMisureQuartorarieSchema.h20_q79)).alias(erpAggregatoOSchema.q79),
      sum(col(aggregazioniMisureQuartorarieSchema.h20_q80)).alias(erpAggregatoOSchema.q80),
      sum(col(aggregazioniMisureQuartorarieSchema.h21_q81)).alias(erpAggregatoOSchema.q81),
      sum(col(aggregazioniMisureQuartorarieSchema.h21_q82)).alias(erpAggregatoOSchema.q82),
      sum(col(aggregazioniMisureQuartorarieSchema.h21_q83)).alias(erpAggregatoOSchema.q83),
      sum(col(aggregazioniMisureQuartorarieSchema.h21_q84)).alias(erpAggregatoOSchema.q84),
      sum(col(aggregazioniMisureQuartorarieSchema.h22_q85)).alias(erpAggregatoOSchema.q85),
      sum(col(aggregazioniMisureQuartorarieSchema.h22_q86)).alias(erpAggregatoOSchema.q86),
      sum(col(aggregazioniMisureQuartorarieSchema.h22_q87)).alias(erpAggregatoOSchema.q87),
      sum(col(aggregazioniMisureQuartorarieSchema.h22_q88)).alias(erpAggregatoOSchema.q88),
      sum(col(aggregazioniMisureQuartorarieSchema.h23_q89)).alias(erpAggregatoOSchema.q89),
      sum(col(aggregazioniMisureQuartorarieSchema.h23_q90)).alias(erpAggregatoOSchema.q90),
      sum(col(aggregazioniMisureQuartorarieSchema.h23_q91)).alias(erpAggregatoOSchema.q91),
      sum(col(aggregazioniMisureQuartorarieSchema.h23_q92)).alias(erpAggregatoOSchema.q92),
      sum(col(aggregazioniMisureQuartorarieSchema.h24_q93)).alias(erpAggregatoOSchema.q93),
      sum(col(aggregazioniMisureQuartorarieSchema.h24_q94)).alias(erpAggregatoOSchema.q94),
      sum(col(aggregazioniMisureQuartorarieSchema.h24_q95)).alias(erpAggregatoOSchema.q95),
      sum(col(aggregazioniMisureQuartorarieSchema.h24_q96)).alias(erpAggregatoOSchema.q96),
      sum(col(aggregazioniMisureQuartorarieSchema.h25_q97)).alias(erpAggregatoOSchema.q97),
      sum(col(aggregazioniMisureQuartorarieSchema.h25_q98)).alias(erpAggregatoOSchema.q98),
      sum(col(aggregazioniMisureQuartorarieSchema.h25_q99)).alias(erpAggregatoOSchema.q99),
      sum(col(aggregazioniMisureQuartorarieSchema.h25_q100)).alias(erpAggregatoOSchema.q100)
    )
      .withColumn(erpAggregatoOSchema.executionid, lit(timestamp))

    // Cast coerente con Hive e format giorno as yyyy-MM-dd
    val dfAggregqqtionTyped = dfAggregqqtion
      .withColumn(aggregazioniMisureQuartorarieSchema.anno, col(aggregazioniMisureQuartorarieSchema.anno).cast("string"))
      .withColumn(aggregazioniMisureQuartorarieSchema.mese, lpad(col(aggregazioniMisureQuartorarieSchema.mese).cast("string"), 2, "0"))
      .withColumn(aggregazioniMisureQuartorarieSchema.giorno, lpad(col(aggregazioniMisureQuartorarieSchema.giorno).cast("string"), 2, "0"))

    val finalDf = dfAggregqqtionTyped
      // Build ISO date string for giorno
      .withColumn(erpAggregatoOSchema.giorno,
        concat_ws("-",
          col(aggregazioniMisureQuartorarieSchema.anno),
          col(aggregazioniMisureQuartorarieSchema.mese),
          col(aggregazioniMisureQuartorarieSchema.giorno)
        )
      )
      .withColumn("origine", lit("TERNA"))
      .selectExpr(erpAggregatoOSchema.getValues: _*)

    finalDf
  }

}
