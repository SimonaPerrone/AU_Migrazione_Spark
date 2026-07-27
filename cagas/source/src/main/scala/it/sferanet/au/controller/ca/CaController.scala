package it.sferanet.au.controller.ca

import it.sferanet.au.model.Flow._
import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.caFinal.TechInfoCa
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.model._
import it.sferanet.au.schema.{CaOutputSchema, RcuGasConnessioniDistr2PSchema, TabProfStdPercSchema}
import it.sferanet.au.utilities.{Constants, DateUtils, Environment}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{broadcast, col, concat, least, lit, max, md5, round, row_number, sum, when}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType

import java.sql.Timestamp
import java.util.{Calendar, Date}

class CaController {

  def getValidRemi(caDF: DataFrame, rcugasConnessioniDistr2Remi: DataFrame): DataFrame = {

    val connessioni = rcugasConnessioniDistr2Remi
      .withColumnRenamed(RcuGasConnessioniDistr2PSchema.t_remi, TabProfStdPercSchema.cod_remi)

    val window = Window
      .partitionBy(col(CaOutputSchema.pdr), col(CaOutputSchema.startSegment), col(CaOutputSchema.endSegment))
      .orderBy(col(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn).desc)

    val joined = caDF.join(
      connessioni,
      caDF(CaOutputSchema.pdr.toString) === connessioni(RcuGasConnessioniDistr2PSchema.t_codice_pdr.toString) &&
        col(CaOutputSchema.endSegment.toString).between(
          connessioni(RcuGasConnessioniDistr2PSchema.d_data_inizio_conn.toString),
          connessioni(RcuGasConnessioniDistr2PSchema.d_data_fine_conn.toString)
        ),
      "left"
    )
      .drop(RcuGasConnessioniDistr2PSchema.t_codice_pdr.toString)

    joined
      .withColumn("rn", row_number().over(window))
      .filter(col("rn") === 1)
      .drop(
        RcuGasConnessioniDistr2PSchema.d_data_inizio_conn.toString,
        RcuGasConnessioniDistr2PSchema.d_data_fine_conn.toString,
        "rn"
      )
  }

  def joinWeights(caDF: DataFrame, weightsDFPre: DataFrame, weightsDFPost: DataFrame): DataFrame = {

    val consumptionExpression = when(col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue) >= 0, col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue)).otherwise(lit(0))
    val consumptionRaw = col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue)
    val f1Expression = consumptionExpression * col(CaOutputSchema.pprofnk_wkr)/col(CaOutputSchema.pprof_ce)
    val f2Expression = consumptionExpression
    val f3Expression = ((consumptionExpression - col("ce"))/col("wkr")) + col("ce")

    val joinFieldsPreRemi = Seq(CaOutputSchema.id_regClim.toString, CaOutputSchema.codiceProfilo.toString)
    val joinFieldsPostRemi = Seq(TabProfStdPercSchema.cod_remi.toString, CaOutputSchema.id_regClim.toString, CaOutputSchema.codiceProfilo.toString)
    val groupByFields = Seq(col(CaOutputSchema.pdr), col("hash_couples"))
    val lastJoinFields = Seq(CaOutputSchema.pdr.toString, "hash_couples")
    val windowPdr = Window.partitionBy(col(CaOutputSchema.pdr))

    val weightsRenamedPre = broadcast(weightsDFPre)
      .withColumnRenamed(TabProfStdPercSchema.id_reg_clim, CaOutputSchema.id_regClim)
      .withColumnRenamed(TabProfStdPercSchema.prof, CaOutputSchema.codiceProfilo)

    val weightsRenamedPost = weightsDFPost
      .withColumnRenamed(TabProfStdPercSchema.id_reg_clim, CaOutputSchema.id_regClim)
      .withColumnRenamed(TabProfStdPercSchema.prof, CaOutputSchema.codiceProfilo)

    val segments = caDF
      .select(CaOutputSchema.pdr, CaOutputSchema.startSegment, CaOutputSchema.endSegment, TabProfStdPercSchema.cod_remi, CaOutputSchema.id_regClim, CaOutputSchema.codiceProfilo)
      .withColumn("hash_couples", md5(concat(col(CaOutputSchema.startSegment), col(CaOutputSchema.endSegment))))

    val weightsPerPdrPre = segments
      .join(weightsRenamedPre, joinFieldsPreRemi, "left")
      .filter(
        col(TabProfStdPercSchema.data) > col(CaOutputSchema.startSegment) &&
          col(TabProfStdPercSchema.data) <= col(CaOutputSchema.endSegment)
      )
      .groupBy(groupByFields: _*)
      .agg(
        sum(when(col(TabProfStdPercSchema.pprofk) >= 0, col(TabProfStdPercSchema.pprofk)).otherwise(lit(0))).as("pprofk_pre"),
        sum(when(col(TabProfStdPercSchema.pprofk_norm) >= 0, col(TabProfStdPercSchema.pprofk_norm)).otherwise(lit(0))).as("pprof_nk_pre"),
        max(when(col(TabProfStdPercSchema.data) === col(CaOutputSchema.endSegment), col("wkr"))).as("wkr_pre")
      )

    val weightsPerPdrPost = segments
      .join(weightsRenamedPost, joinFieldsPostRemi, "left")
      .filter(
        col(TabProfStdPercSchema.data) > col(CaOutputSchema.startSegment) &&
          col(TabProfStdPercSchema.data) <= col(CaOutputSchema.endSegment)
      )
      .groupBy(groupByFields:_*)
      .agg(
        sum(when(col(TabProfStdPercSchema.pprofk) >= 0, col(TabProfStdPercSchema.pprofk)).otherwise(lit(0))).as("pprofk_post"),
        sum(when(col(TabProfStdPercSchema.pprofk_norm) >= 0, col(TabProfStdPercSchema.pprofk_norm)).otherwise(lit(0))).as("pprof_nk_post"),
        max(when(col(TabProfStdPercSchema.data) === col(CaOutputSchema.endSegment), col("wkr"))).as("wkr_post")
      )

    val updatedCaDF = caDF
      .withColumn("hash_couples", md5(concat(col(CaOutputSchema.startSegment), col(CaOutputSchema.endSegment))))
      .join(weightsPerPdrPre, lastJoinFields, "left")
      .join(weightsPerPdrPost, lastJoinFields, "left")
      .withColumn(
        CaOutputSchema.pprof_ce,
        when(col("pprofk_pre").isNotNull && col("pprofk_post").isNotNull,
          col("pprofk_pre") + col("pprofk_post"))
          .when(col("pprofk_pre").isNotNull && col("pprofk_post").isNull,
            col("pprofk_pre"))
          .when(col("pprofk_pre").isNull && col("pprofk_post").isNotNull,
            col("pprofk_post"))
          .otherwise(lit(null))
      )
      .withColumn(
        CaOutputSchema.pprofnk_wkr,
        when(col("pprof_nk_pre").isNotNull && col("pprof_nk_post").isNotNull,
          col("pprof_nk_pre") + col("pprof_nk_post"))
          .when(col("pprof_nk_pre").isNotNull && col("pprof_nk_post").isNull,
            col("pprof_nk_pre"))
          .when(col("pprof_nk_pre").isNull && col("pprof_nk_post").isNotNull,
            col("pprof_nk_post"))
          .otherwise(lit(null))
      )
      .withColumn(
        "wkr",
        when(col("wkr_pre").isNotNull && col("wkr_post").isNotNull,
          col("wkr_pre") + col("wkr_post"))
          .when(col("wkr_pre").isNotNull && col("wkr_post").isNull,
            col("wkr_pre"))
          .when(col("wkr_pre").isNull && col("wkr_post").isNotNull,
            col("wkr_post"))
          .otherwise(lit(null))
      )
      .withColumn("indet_flag", when(col(CaOutputSchema.pprofnk_wkr) === 0, lit(true)).otherwise(lit(false)))
      .withColumn("negative_pprofnk_flag", when(col(CaOutputSchema.pprofnk_wkr) < 0, lit(true)).otherwise(lit(false)))
      .withColumn("negative_pprofce_flag", when(col(CaOutputSchema.pprof_ce) < 0, lit(true)).otherwise(lit(false)))
      .withColumn("negative_consumption_flag", when(col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue) < 0, lit(true)).otherwise(lit(false)))
      .withColumn("ce", least(f2Expression, col("ce_mean")))
      .withColumn(CaOutputSchema.idCaErrorCode, when(
        (col(CaOutputSchema.caMethods) === CAMethods.DailyHeat.id) &&
          col("ce").isNull
        , lit(CAErrorCode.MissingCe.id)).otherwise(col(CaOutputSchema.idCaErrorCode)))
      .withColumn(
        CaOutputSchema.caValue,
        when(col(CaOutputSchema.caMethods) === CAMethods.Monthly.id && !col("indet_flag"), f1Expression)
          .when(col(CaOutputSchema.caMethods) === CAMethods.Daily.id, f2Expression)
          .when(col(CaOutputSchema.caMethods) === CAMethods.DailyHeat.id && col(CaOutputSchema.idCaErrorCode) =!= CAErrorCode.MissingCe.id, f3Expression)
          .otherwise(lit(0.0))
      )
      .withColumn(CaOutputSchema.idCaErrorCode, when(
        (col("negative_pprofnk_flag")
          .or(col("negative_pprofce_flag"))
          .or(col("negative_consumption_flag"))
          .or(consumptionRaw < 0))
          .and(!col(CaOutputSchema.idCaErrorCode)
            .isin(CAErrorCode.MissingMeasure.id, CAErrorCode.ContractDiscontinuity.id, CAErrorCode.MissingRcu.id, CAErrorCode.MissingCe.id))
        , lit(CAErrorCode.NegativeConsumption.id)
      ).otherwise(col(CaOutputSchema.idCaErrorCode)))
      .withColumn(CaOutputSchema.idCaErrorCode, when(
        (col(CaOutputSchema.pprof_ce).isNull.or(col(CaOutputSchema.pprofnk_wkr).isNull))
          .and(!col(CaOutputSchema.idCaErrorCode).isin(CAErrorCode.MissingMeasure.id, CAErrorCode.ContractDiscontinuity.id, CAErrorCode.MissingRcu.id, CAErrorCode.MissingCe.id, CAErrorCode.NegativeConsumption.id))
        , lit(CAErrorCode.MissingWeight.id)
      ).otherwise(col(CaOutputSchema.idCaErrorCode)))
      .withColumn(CaOutputSchema.caValue, when(
        col(CaOutputSchema.idCaErrorCode) =!= CAErrorCode.None.id, lit(0.0)
      ).otherwise(col(CaOutputSchema.caValue)))
      .withColumn(CaOutputSchema.ca_sum, sum(round(col(CaOutputSchema.caValue))).over(windowPdr))
      .withColumn(CaOutputSchema.ca_sum, col(CaOutputSchema.ca_sum).cast(LongType))

    updatedCaDF
  }

  def applyForcedRecalculation(df: DataFrame): DataFrame = {
    val codProfilo = col(CaOutputSchema.next_cod_profilo)
    val caSum = col(CaOutputSchema.ca_sum)
    val zonaClimaticaLookup = col("zona_climatica_lookup")

    // replica getCatUso(ca)
    val catUsoAtteso = when(caSum < 500, lit("C2"))
      .when(caSum <= 5000, lit("C3"))
      .otherwise(lit("C1"))

    // replica getCompTermica(catUsoAtteso)
    val compTermica = catUsoAtteso.isin("C1", "C3", "C5", "T2")

    val catUso = codProfilo.substr(1, 2)
    val zc = codProfilo.substr(3, 1)
    val classePrelievo = codProfilo.substr(4, 1)

    // primo if: profilo != null && ca > 0 && startsWith("C")
    val firstCondition =
      codProfilo.isNotNull &&
        caSum > 0 &&
        codProfilo.substr(1, 1) === "C"

    // secondo if annidato: catUso != catUsoAtteso
    val secondCondition = catUso =!= catUsoAtteso

    // replica zonaClim — calcolata solo quando firstCondition && secondCondition
    val zonaClim = when(firstCondition && secondCondition,
      when(compTermica,
        when(zc === "X",
          when(zonaClimaticaLookup.isNotNull, zonaClimaticaLookup).otherwise(lit(""))
        ).otherwise(zc)
      ).otherwise(lit("X"))
    ).otherwise(lit(""))

    // terzo if annidato: zonaClim != ""
    val needsRecalc = firstCondition && secondCondition && zonaClim =!= ""

    // nuovo codice profilo forzato
    val newCodProfilo = when(needsRecalc,
      concat(catUsoAtteso, zonaClim, classePrelievo)
    ).otherwise(codProfilo)

    df
      .withColumn("needsRecalc", needsRecalc)
      .withColumn(CaOutputSchema.next_cod_profilo, newCodProfilo)
  }

  def joinSchedaTds(df: DataFrame, tds: DataFrame): DataFrame = {
    val tdsBroadcast = broadcast(tds.withColumnRenamed("cod_pdr", "pdr"))

    val joined =
      df
        .join(tdsBroadcast, Seq(CaOutputSchema.pdr.toString), "left")

    val nextCodProfilo = col(CaOutputSchema.next_cod_profilo.toString)
    val zonaClimaticaLookup = col("zona_climatica_lookup")
    val catUsoTds = col("cat_uso")
    val classePrelievoTds = col("classe_prelievo")
    val presTds = catUsoTds.isNotNull

    val tipologia = nextCodProfilo.substr(1, 1) === "C"

    // caso schedaTds definita
    val compTermicaTds = catUsoTds.isin("C1", "C3", "C5", "T2")

    val codProfTds = when(compTermicaTds,
      when(zonaClimaticaLookup.isNotNull,
        concat(catUsoTds, zonaClimaticaLookup, classePrelievoTds)
      ).otherwise(nextCodProfilo)
    ).otherwise(concat(catUsoTds, lit("X"), classePrelievoTds))

    val profModeTds = when(compTermicaTds,
      when(zonaClimaticaLookup.isNotNull, lit(ProfStdMode.Tds.id))
        .otherwise(lit(ProfStdMode.LastRcu.id))
    ).otherwise(lit(ProfStdMode.Tds.id))

    // caso schedaTds non definita
    val catUsoCalc = when(tipologia,
      when(col(CaOutputSchema.ca_sum.toString) < 500, lit("C2"))
        .when(col(CaOutputSchema.ca_sum.toString) <= 5000, lit("C3"))
        .otherwise(lit("C1"))
    ).otherwise(lit("T2"))

    val classePrelievoCalc = when(catUsoCalc === "T2", lit("3")).otherwise(lit("1"))

    val compTermicaCalc = catUsoCalc.isin("C1", "C3", "C5", "T2")

    val codProfCalc = when(compTermicaCalc,
      when(zonaClimaticaLookup.isNotNull,
        concat(catUsoCalc, zonaClimaticaLookup, classePrelievoCalc)
      ).otherwise(nextCodProfilo)
    ).otherwise(concat(catUsoCalc, lit("X"), classePrelievoCalc))

    val profModeCalc = when(compTermicaCalc,
      when(zonaClimaticaLookup.isNotNull, lit(ProfStdMode.Calculated.id))
        .otherwise(lit(ProfStdMode.LastRcu.id))
    ).otherwise(lit(ProfStdMode.Calculated.id))

    // merge dei due casi
    val codProf = when(presTds, codProfTds).otherwise(col(CaOutputSchema.codiceProfilo))
    val profMode = when(presTds, profModeTds).otherwise(profModeCalc)
    val catUsoFinal = when(presTds, catUsoTds).otherwise(catUsoCalc)
    val compTermicaFinal = when(presTds, compTermicaTds).otherwise(compTermicaCalc)
    val classePrelievoFinal = when(presTds, lit(null).cast("string")).otherwise(classePrelievoCalc)

    joined
      .withColumn(CaOutputSchema.codiceProfilo, codProf)
      .withColumn(CaOutputSchema.profMode, profMode)
      .withColumn("comp_termica", compTermicaFinal)
      .withColumn("cat_uso_tds", catUsoFinal)
      .withColumn("classe_prelievo_tds", classePrelievoFinal)
      .withColumn("pres_tds", presTds)
      .withColumn("tipologia_uso", tipologia)
  }

  def executeRecalculation(df: DataFrame, weightsDFPreRemi: DataFrame, weightsDFPostRemi: DataFrame): DataFrame = {
    val consumptionExpression = when(col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue) >= 0, col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue)).otherwise(lit(0))
    val consumptionRaw = col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue)
    val f1Expression = consumptionExpression * col(CaOutputSchema.pprofnk_wkr) / col(CaOutputSchema.pprof_ce)
    val f2Expression = consumptionExpression
    val f3Expression = ((consumptionExpression - col("ce")) / col("wkr")) + col("ce")

    val needsRecalc = df.filter(col("needsRecalc")).drop("pprofk_pre", "pprof_nk_pre", "wkr_pre", "pprofk_post", "pprof_nk_post", "wkr_post")
    val noNeedsRecalc = df.filter(!col("needsRecalc"))

    val joinFieldsPreRemi = Seq(CaOutputSchema.id_regClim.toString, CaOutputSchema.next_cod_profilo.toString)
    val joinFieldsPostRemi = Seq(TabProfStdPercSchema.cod_remi.toString, CaOutputSchema.id_regClim.toString, CaOutputSchema.next_cod_profilo.toString)
    val groupByFields = Seq(col(CaOutputSchema.pdr), col("hash_couples"))
    val lastJoinFields = Seq(CaOutputSchema.pdr.toString, "hash_couples")
    val windowPdr = Window.partitionBy(col(CaOutputSchema.pdr))

    val weightsRenamedPre = broadcast(weightsDFPreRemi)
      .withColumnRenamed(TabProfStdPercSchema.id_reg_clim, CaOutputSchema.id_regClim)
      .withColumnRenamed(TabProfStdPercSchema.prof, CaOutputSchema.next_cod_profilo)

    val weightsRenamedPost = weightsDFPostRemi
      .withColumnRenamed(TabProfStdPercSchema.id_reg_clim, CaOutputSchema.id_regClim)
      .withColumnRenamed(TabProfStdPercSchema.prof, CaOutputSchema.next_cod_profilo)

    val segments = needsRecalc
      .select(CaOutputSchema.pdr, CaOutputSchema.startSegment, CaOutputSchema.endSegment, TabProfStdPercSchema.cod_remi, CaOutputSchema.id_regClim, CaOutputSchema.next_cod_profilo)
      .withColumn("hash_couples", md5(concat(col(CaOutputSchema.startSegment), col(CaOutputSchema.endSegment))))

    val weightsPerPdrPre = segments
      .join(weightsRenamedPre, joinFieldsPreRemi, "left")
      .filter(
        col(TabProfStdPercSchema.data) > col(CaOutputSchema.startSegment) &&
          col(TabProfStdPercSchema.data) <= col(CaOutputSchema.endSegment)
      )
      .groupBy(groupByFields: _*)
      .agg(
        sum(when(col(TabProfStdPercSchema.pprofk) >= 0, col(TabProfStdPercSchema.pprofk)).otherwise(lit(0))).as("pprofk_pre"),
        sum(when(col(TabProfStdPercSchema.pprofk_norm) >= 0, col(TabProfStdPercSchema.pprofk_norm)).otherwise(lit(0))).as("pprof_nk_pre"),
        max(when(col(TabProfStdPercSchema.data) === col(CaOutputSchema.endSegment), col("wkr"))).as("wkr_pre")
      )

    val weightsPerPdrPost = segments
      .join(weightsRenamedPost, joinFieldsPostRemi, "left")
      .filter(
        col(TabProfStdPercSchema.data) > col(CaOutputSchema.startSegment) &&
          col(TabProfStdPercSchema.data) <= col(CaOutputSchema.endSegment)
      )
      .groupBy(groupByFields: _*)
      .agg(
        sum(when(col(TabProfStdPercSchema.pprofk) >= 0, col(TabProfStdPercSchema.pprofk)).otherwise(lit(0))).as("pprofk_post"),
        sum(when(col(TabProfStdPercSchema.pprofk_norm) >= 0, col(TabProfStdPercSchema.pprofk_norm)).otherwise(lit(0))).as("pprof_nk_post"),
        max(when(col(TabProfStdPercSchema.data) === col(CaOutputSchema.endSegment), col("wkr"))).as("wkr_post")
      )

    val recalcDF = needsRecalc
      .withColumn("hash_couples", md5(concat(col(CaOutputSchema.startSegment), col(CaOutputSchema.endSegment))))
      .join(weightsPerPdrPre, lastJoinFields, "left")
      .join(weightsPerPdrPost, lastJoinFields, "left")
      .withColumn(
        CaOutputSchema.pprof_ce,
        when(col("pprofk_pre").isNotNull && col("pprofk_post").isNotNull,
          col("pprofk_pre") + col("pprofk_post"))
          .when(col("pprofk_pre").isNotNull && col("pprofk_post").isNull,
            col("pprofk_pre"))
          .when(col("pprofk_pre").isNull && col("pprofk_post").isNotNull,
            col("pprofk_post"))
          .otherwise(lit(null))
      )
      .withColumn(
        CaOutputSchema.pprofnk_wkr,
        when(col("pprof_nk_pre").isNotNull && col("pprof_nk_post").isNotNull,
          col("pprof_nk_pre") + col("pprof_nk_post"))
          .when(col("pprof_nk_pre").isNotNull && col("pprof_nk_post").isNull,
            col("pprof_nk_pre"))
          .when(col("pprof_nk_pre").isNull && col("pprof_nk_post").isNotNull,
            col("pprof_nk_post"))
          .otherwise(lit(null))
      )
      .withColumn(
        "wkr",
        when(col("wkr_pre").isNotNull && col("wkr_post").isNotNull,
          col("wkr_pre") + col("wkr_post"))
          .when(col("wkr_pre").isNotNull && col("wkr_post").isNull,
            col("wkr_pre"))
          .when(col("wkr_pre").isNull && col("wkr_post").isNotNull,
            col("wkr_post"))
          .otherwise(lit(null))
      )
      .withColumn("indet_flag", when(col(CaOutputSchema.pprofnk_wkr) === 0, lit(true)).otherwise(lit(false)))
      .withColumn("negative_pprofnk_flag", when(col(CaOutputSchema.pprofnk_wkr) < 0, lit(true)).otherwise(lit(false)))
      .withColumn("negative_pprofce_flag", when(col(CaOutputSchema.pprof_ce) < 0, lit(true)).otherwise(lit(false)))
      .withColumn("negative_consumption_flag", when(col(CaOutputSchema.endValue) - col(CaOutputSchema.startValue) < 0, lit(true)).otherwise(lit(false)))
      .withColumn("ce", least(f2Expression, col("ce_mean")))
      .withColumn(CaOutputSchema.idCaErrorCode, when(
        (col(CaOutputSchema.caMethods) === CAMethods.DailyHeat.id) &&
          col("ce").isNull
        , lit(CAErrorCode.MissingCe.id)).otherwise(col(CaOutputSchema.idCaErrorCode)))
      .withColumn(
        CaOutputSchema.caValue,
        when(col(CaOutputSchema.caMethods) === CAMethods.Monthly.id && !col("indet_flag"), f1Expression)
          .when(col(CaOutputSchema.caMethods) === CAMethods.Daily.id, f2Expression)
          .when(col(CaOutputSchema.caMethods) === CAMethods.DailyHeat.id && col(CaOutputSchema.idCaErrorCode) =!= CAErrorCode.MissingCe.id, f3Expression)
          .otherwise(col(CaOutputSchema.caValue))
      )
      .withColumn(CaOutputSchema.idCaErrorCode, when(
        (col("negative_pprofnk_flag")
          .or(col("negative_pprofce_flag"))
          .or(col("negative_consumption_flag"))
          .or(consumptionRaw < 0))
          .and(!col(CaOutputSchema.idCaErrorCode).isin(CAErrorCode.MissingMeasure.id, CAErrorCode.ContractDiscontinuity.id, CAErrorCode.MissingRcu.id, CAErrorCode.MissingCe.id))
        , lit(CAErrorCode.NegativeConsumption.id)
      ).otherwise(col(CaOutputSchema.idCaErrorCode)))
      .withColumn(CaOutputSchema.idCaErrorCode, when(
        (col(CaOutputSchema.pprof_ce).isNull.or(col(CaOutputSchema.pprofnk_wkr).isNull))
          .and(!col(CaOutputSchema.idCaErrorCode).isin(CAErrorCode.MissingMeasure.id, CAErrorCode.ContractDiscontinuity.id, CAErrorCode.MissingRcu.id, CAErrorCode.MissingCe.id, CAErrorCode.NegativeConsumption.id))
        , lit(CAErrorCode.MissingWeight.id)
      ).otherwise(col(CaOutputSchema.idCaErrorCode)))
      .withColumn(CaOutputSchema.caValue, when(
        col(CaOutputSchema.idCaErrorCode) =!= CAErrorCode.None.id, lit(0.0)
      ).otherwise(col(CaOutputSchema.caValue)))
      .withColumn(CaOutputSchema.ca_sum, sum(round(col(CaOutputSchema.caValue))).over(windowPdr))
      .withColumn(CaOutputSchema.ca_sum, col(CaOutputSchema.ca_sum).cast(LongType))

    recalcDF.unionByName(noNeedsRecalc)
  }

  def execute(consumption: RDD[(String, (IndexedSeq[Consumption], Iterable[RcuGasMassivoTech], Iterable[RcuGasMassivo], Double, Iterable[RcuGasProfilo]))],
              lookupZonaClimatica: Broadcast[scala.collection.Map[String, String]]
             ): RDD[(String, Iterable[(Consumption, CAErrorCode.Value, CaParameter)], ProfStdMode.Value, Option[TechInfoCa])] = {

    val endContractContinuity = Constants.getDate(Constants.getFormatter("yyyy-MM-dd"), Environment.getContractContuinityUpperBoundDate).get

    consumption
      .filter(v => v._2 != null && v._2._1.nonEmpty)
      .map {
        case (pdr, (c, rcusTech, rcus, ceMean, rcusProfilo)) =>
          val consumptions = c.filter(cons => cons.idConsumptionErrorState == ConsumptionErrorStates.None
            || cons.idConsumptionErrorState == ConsumptionErrorStates.SerialNumberMismatch)

          val lastrcu = rcus.maxBy(_.t_anno_termico)
          val profiloStdRcuVarOption = RcuGasProfilo.getRecordAtDate(rcusProfilo, endContractContinuity).map(_.t_cod_profilo)
          val profilo_std_last_rcu = profiloStdRcuVarOption.getOrElse("")

          if (profiloStdRcuVarOption.isEmpty) {
            (pdr, List[(Consumption, CAErrorCode.Value, CaParameter)]((Consumption.empty(pdr), CAErrorCode.MissingRcu, new CaParameter(CAMethods.MissingRcu, lastrcu.t_cod_profilo, lastrcu.t_cod_profilo ,lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr))), ProfStdMode.LastRcu, None)
          }
          else if (consumptions.isEmpty) {
            (pdr, List[(Consumption, CAErrorCode.Value, CaParameter)]((Consumption.empty(pdr), CAErrorCode.MissingMeasure, new CaParameter(CAMethods.MissingRcu, lastrcu.t_cod_profilo, lastrcu.t_cod_profilo ,lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr))), ProfStdMode.LastRcu, None)
          } else {

            val methods = CaController.getCaMethods(consumptions, rcusProfilo)

            if (methods.isEmpty)
              (pdr, List[(Consumption, CAErrorCode.Value, CaParameter)]((Consumption.empty(pdr), CAErrorCode.MissingMethods, new CaParameter(CAMethods.MissingRcu, lastrcu.t_cod_profilo, lastrcu.t_cod_profilo, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr))), ProfStdMode.LastRcu, None)
            else if (!CaController.getContractContinuity(methods, rcus, endContractContinuity)) {
              (pdr, List[(Consumption, CAErrorCode.Value, CaParameter)]((Consumption.empty(pdr), CAErrorCode.ContractDiscontinuity, new CaParameter(CAMethods.NoSuchTypeConsume, lastrcu.t_cod_profilo, lastrcu.t_cod_profilo, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr))), ProfStdMode.LastRcu, None)
            }
            else {
              val CAValues = CaController.getCaValues(methods, profilo_std_last_rcu, lastrcu, rcusProfilo)
              val zonaClimaticaLookup = lookupZonaClimatica.value.get(lastrcu.t_comune_istat_pdr)

              val techInfo = TechInfoCa(
                pres_tds = false, tipologia_uso = false, comp_termica = false, cat_uso_tds = "",
                classe_prelievo_tds = Some(""), cod_istat_last_rcu = lastrcu.t_comune_istat_pdr, zona_climatica_lookup = zonaClimaticaLookup,
                ce_mean = Some(ceMean)
              )
              (pdr, CAValues, ProfStdMode.Wip, Some(techInfo))
            }
          }
      }
  }
}

object CaController {
  def getCaTotValue(CAValues: Iterable[(Consumption, Double, CAErrorCode.Value, CaParameter)]): Long = {
    CAValues.map(v => Math.round(v._2)).sum
  }

  def getCompTermica(catUso: String): Boolean = {
    catUso == "C1" || catUso == "C3" || catUso == "C5" || catUso == "T2"
  }

  def getCatUso(caValue: Long): String = {
    if (caValue < 500)
      "C2"
    else if (caValue <= 5000)
      "C3"
    else
      "C1"
  }

  def getCaValues(methods: scala.IndexedSeq[(Consumption, CAMethods.Value)],
                  profilo_std_last_rcu: String,
                  lastrcu: RcuGasMassivo,
                  rcusProfilo: Iterable[RcuGasProfilo]): Iterable[(Consumption, CAErrorCode.Value, CaParameter)] = {

    methods.groupBy(v => v._2).flatMap {
      case (methods, c) =>
        val consumptions = c.map(_._1)

        val res = methods match {
          case CAMethods.Monthly =>
            consumptions.map(v => {
              val current_t_cod_profilo = getCurrentTCodProfilo(v.endSegment, rcusProfilo)

              (v, CAErrorCode.None, new CaParameter(CAMethods.Monthly, current_t_cod_profilo, profilo_std_last_rcu, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr))

            })

          case CAMethods.Daily =>
            consumptions.map(v => {
              val current_t_cod_profilo = getCurrentTCodProfilo(v.endSegment, rcusProfilo)
                (v, CAErrorCode.None, new CaParameter(CAMethods.Daily, current_t_cod_profilo, profilo_std_last_rcu, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr))
            })

          case CAMethods.DailyHeat =>
            consumptions.map(v => {
              val current_t_cod_profilo = getCurrentTCodProfilo(v.endSegment, rcusProfilo)
              (v, CAErrorCode.None, new CaParameter(CAMethods.DailyHeat, current_t_cod_profilo, profilo_std_last_rcu, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr))
            })

          case CAMethods.NoSuchTypeConsume =>
            consumptions.map(v => (v, CAErrorCode.MissingMethods, new CaParameter(CAMethods.NoSuchTypeConsume, lastrcu.t_cod_profilo, lastrcu.t_cod_profilo, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr)))
          case CAMethods.MissingMZInterval =>
            consumptions.map(v => (v, CAErrorCode.MissingMZInterval, new CaParameter(CAMethods.MissingMZInterval,lastrcu.t_cod_profilo, lastrcu.t_cod_profilo, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr)))
          case CAMethods.MissingMeasure =>
            consumptions.map(v => (v, CAErrorCode.MissingMeasure, new CaParameter(CAMethods.MissingMeasure,lastrcu.t_cod_profilo, lastrcu.t_cod_profilo, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr)))
          case CAMethods.MissingRcu =>
            consumptions.map(v => (v, CAErrorCode.MissingRcu, new CaParameter(CAMethods.MissingRcu, lastrcu.t_cod_profilo, lastrcu.t_cod_profilo, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr)))
          case CAMethods.SerialNumberMismatch =>
            consumptions.map(v => (v, CAErrorCode.SerialNumberMismatch, new CaParameter(CAMethods.SerialNumberMismatch, lastrcu.t_cod_profilo, lastrcu.t_cod_profilo, lastrcu.id_regione_climatica, lastrcu.t_comune_istat_pdr)))
        }

        res
    }
  }

  def getCurrentTCodProfilo(endSegmentDate: Date, rcusProfilo: Iterable[RcuGasProfilo]): String = {
    RcuGasProfilo.getRecordAtDate(rcusProfilo, endSegmentDate).map(_.t_cod_profilo).orNull
  }

  def getContractContinuity(methods: IndexedSeq[(Consumption, CAMethods.Value)], rcus: Iterable[RcuGasMassivo], endDate: Date): Boolean = {

    val startDate = methods.last._1.startSegment
    val discontinuty = rcus.filter(ruc => // rcustart >=zstart && rcustart <= zend
      (ruc.startDate == endDate || ruc.startDate.before(endDate)) &&
        ruc.startDate.after(startDate)
    ).exists(rcu => rcu.t_processo == "VTG" ||
      rcu.t_processo == "VSG" ||
      rcu.t_processo == "VA") ||
      methods.exists(f => f._1.endService == "TMV" || f._1.endService == "A01")
    !discontinuty
  }

  def getCaMethods(consumptions: IndexedSeq[Consumption], rcus: Iterable[RcuGasProfilo]): IndexedSeq[(Consumption, CAMethods.Value)] = {
    // recupero la prima misura (quindi la misura più recente
    val endZ = Calendar.getInstance()

    endZ.setTime(consumptions.head.endSegment)
    val start = new Timestamp(endZ.getTimeInMillis)
    // vado indietro di un anno
    endZ.add(Calendar.YEAR, -1)
    endZ.set(Calendar.HOUR_OF_DAY, 0)
    endZ.set(Calendar.MINUTE, 0)
    endZ.set(Calendar.SECOND, 0)
    endZ.set(Calendar.MILLISECOND, 0)

    val endDateZ = new Timestamp(endZ.getTimeInMillis)

    val consuWithEndDateZ = consumptions.map(c => c.copy(endDateZ = endDateZ, startDateZ = start))

    // seleziono i consumi dell'ultimo anno
    val consumptionsA = consuWithEndDateZ.filter(v => v.startSegment.after(endDateZ) || v.startSegment == endDateZ)
    // aggiungo ai consumi dell'ultimo anno un altro segmento andando indietro.
    // calcoliamo l'insieme Z aggiungendo un ulteriore segnante
    val (consumptionsZ, hasZ) = if (consuWithEndDateZ.length > consumptionsA.length) {
      (consumptionsA :+ consuWithEndDateZ(consumptionsA.length), true)
    } else {
      (consumptionsA, false)
    }

    val typesFlow = consumptionsA.map(_.endService).distinct
    val formula23Services = Set(
      Tgl.serviceName,
      Rgl.serviceName,
      Im1Pre.serviceName,
      Im1Post.serviceName,
      Sw1.serviceName,
      Rsl.serviceName,
      Swg1.serviceName,
      IgmgPre.serviceName,
      IgmgPost.serviceName,
      IgmrPre.serviceName,
      IgmrPost.serviceName,
      FUI.serviceName,
      FDD.serviceName
    )

    if (consumptionsZ.exists(_.idConsumptionErrorState == ConsumptionErrorStates.SerialNumberMismatch)) { // caso SerialNumberMismatch
      if (consumptionsZ.exists(_.idConsumptionErrorState != ConsumptionErrorStates.SerialNumberMismatch))
        consumptionsZ.filter(c => c.idConsumptionErrorState != ConsumptionErrorStates.SerialNumberMismatch).map(c => (c, CAMethods.SerialNumberMismatch))
      else
        consumptionsZ.map(c => (c, CAMethods.SerialNumberMismatch))
    } else if ((consumptionsA.length > 1 && typesFlow.containsTypes(formula23Services)) || // formula 2 o 3
      (
        (consumptionsA.length == 1 && formula23Services.contains(consumptionsA.head.startService))
          && formula23Services.contains(consumptionsA.head.endService)
        )) {
      val first = consumptionsA.minBy(_.startSegment.getTime)
      val cal1 = Calendar.getInstance()
      val cal2 = Calendar.getInstance()
      cal1.setTime(first.startSegment)
      cal2.setTime(endDateZ)

      if (cal1.get(Calendar.DAY_OF_YEAR) != cal2.get(Calendar.DAY_OF_YEAR) ||
        cal1.get(Calendar.YEAR) != cal2.get(Calendar.YEAR)) {
        consumptionsA.map(v => (v, CAMethods.MissingMZInterval))
      } else {
        consumptionsA.map(v => {
          val rcu = RcuGasProfilo.getRecordAtDate(rcus, v.endSegment).orNull
          if (rcu == null || rcu.t_cod_profilo == null)
            (v, CAMethods.MissingRcu)
          else
            (v, if ( rcu.checkFormula2()) CAMethods.Daily else CAMethods.DailyHeat)
        })
      }
    } else { // formula 1
      if (typesFlow.containsTypes(List(
        Tml.serviceName,
        Rml.serviceName,
        Tal.serviceName,
        Tas.serviceName,
        Tav.serviceName,
        Rmv.serviceName,
        Rsl.serviceName,
        Sw1.serviceName,
        Tmv.serviceName,
        A01.serviceName,
        A40.serviceName,
        Im1Post.serviceName,
        Im1Pre.serviceName,
        Sm1.serviceName,
        SM1R.serviceName,
        A01R.serviceName,
        A40R.serviceName,
        Swg1.serviceName,
        IgmgPre.serviceName,
        IgmgPost.serviceName,
        IgmrPre.serviceName,
        IgmrPost.serviceName,
        FUI.serviceName,
        FDD.serviceName,
        AD2.serviceName,
        AD2R.serviceName,
        AD3.serviceName,
        AD3R.serviceName,
        AD4.serviceName,
        AD4R.serviceName,
        AD5.serviceName,
        AD5R.serviceName,
        A02.serviceName,
        A02R.serviceName,
        S02.serviceName,
        S02R.serviceName,
        S40.serviceName,
        S40R.serviceName,
        R01.serviceName,
        R01r.serviceName,
        R40.serviceName,
        R40r.serviceName,
        M01.serviceName,
        M01r.serviceName,
        V01.serviceName,
        V01R.serviceName,
        V02.serviceName,
        V02R.serviceName
      ))
      ) {
        // ***********  formula 1  ***********
        if (hasZ)
          consumptionsZ.map(v => (v, CAMethods.Monthly))
        else
          consumptionsZ.map(v => (v, CAMethods.MissingMZInterval))
      } else {
        // ***********  formula ibrida  ***********
        if (!hasZ)
          consumptionsZ.map(v => (v, CAMethods.MissingMZInterval))
        else {
          for (consumption <- consumptionsZ) yield {
            val rcu: RcuGasProfilo = RcuGasProfilo.getRecordAtDate(rcus, consumption.endSegment).orNull
            if ( // controllo che siano flussi di tipo TML o RML
              (consumption.startService match {
                case Im1Post.serviceName | IgmgPost.serviceName | IgmrPost.serviceName |
                     A01.serviceName | A40.serviceName | Sm1.serviceName | Tal.serviceName | Tas.serviceName | Tml.serviceName | Rmv.serviceName | Rsl.serviceName | Sw1.serviceName | Swg1.serviceName | Tmv.serviceName | Tml.serviceName | Rml.serviceName | FUI.serviceName | FDD.serviceName | A01R.serviceName | A40R.serviceName | SM1R.serviceName | AD2.serviceName | AD2R.serviceName | AD3.serviceName | AD3R.serviceName | AD4.serviceName | AD4R.serviceName | AD5.serviceName | AD5R.serviceName | A02.serviceName | A02R.serviceName | S02.serviceName | S02R.serviceName | S40.serviceName | S40R.serviceName | R01.serviceName | R01r.serviceName | R40.serviceName | R40r.serviceName | M01.serviceName | M01r.serviceName | V01.serviceName | V01R.serviceName | V02.serviceName | V02R.serviceName
                => true
                case _ => false
              }) &&
                (consumption.endService match {
                  case Im1Pre.serviceName | IgmgPre.serviceName | IgmrPre.serviceName |
                       A01.serviceName | A40.serviceName | Sm1.serviceName | Tal.serviceName | Tas.serviceName | Tml.serviceName | Rmv.serviceName | Rsl.serviceName | Sw1.serviceName | Swg1.serviceName | Tmv.serviceName | Tml.serviceName | Rml.serviceName | FUI.serviceName | FDD.serviceName | A01R.serviceName | A40R.serviceName | SM1R.serviceName | AD2.serviceName | AD2R.serviceName | AD3.serviceName | AD3R.serviceName | AD4.serviceName | AD4R.serviceName | AD5.serviceName | AD5R.serviceName | A02.serviceName | A02R.serviceName | S02.serviceName | S02R.serviceName | S40.serviceName | S40R.serviceName | R01.serviceName | R01r.serviceName | R40.serviceName | R40r.serviceName | M01.serviceName | M01r.serviceName | V01.serviceName | V01R.serviceName | V02.serviceName | V02R.serviceName
                  => true
                  case _ => false
                })
                ||
                (
                  (consumption.startService == Tml.serviceName && consumption.endService == Tgl.serviceName) ||
                    (consumption.startService == Tgl.serviceName && consumption.endService == Tml.serviceName)
                  ) && consumption.getNumberOfDays > 1 && hasZ
            ) {
              (consumption, CAMethods.Monthly) // Applico la formula 1

            } else if ( // controllo che siano flussi di tipo TGL o RGL
              (consumption.startService match {
                case Im1Post.serviceName | IgmgPost.serviceName | IgmrPost.serviceName |
                     Rsl.serviceName | Sw1.serviceName | Tgl.serviceName | Rgl.serviceName | Swg1.serviceName | FUI.serviceName | FDD.serviceName
                => true
                case _ => false
              }) &&
                (consumption.endService match {
                  case Im1Pre.serviceName | IgmgPre.serviceName | IgmrPre.serviceName |
                       Rsl.serviceName | Sw1.serviceName | Tgl.serviceName | Rgl.serviceName | Swg1.serviceName | FUI.serviceName | FDD.serviceName
                  => true
                  case _ => false
                })
            ) {
              // in caso di giornaliero distinguo tra i due possibili scenari di calcolo formula 2 o 3
              if (rcu == null || rcu.t_cod_profilo == null)
                (consumption, CAMethods.MissingRcu)
              else if (rcu.checkFormula2()) {
                (consumption, CAMethods.Daily)
              } else {
                (consumption, CAMethods.DailyHeat)
              }
            } else if ( // caso eccezione in cui ci sia un tgl e tml o viceversa con una differenza di un giorno.In questo caso viene considerato come se fosse un tgl.
              (consumption.startService == Tml.serviceName && consumption.endService == Tgl.serviceName ||
                consumption.startService == Tgl.serviceName && consumption.endService == Tml.serviceName) &&
                consumption.getNumberOfDays == 1
            ) {
              if (rcu == null || rcu.t_cod_profilo == null)
                (consumption, CAMethods.MissingRcu)
              else if (rcu.checkFormula2()) {
                (consumption, CAMethods.Daily)
              } else {
                (consumption, CAMethods.DailyHeat)
              }
            }
            else
              (consumption, CAMethods.NoSuchTypeConsume)
          }
        }
      }
    }
  }
}