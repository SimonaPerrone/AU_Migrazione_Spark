package it.eng.au.ERP.trasformations.NO

import it.eng.au.ERP.schema.erp.{erpConsumptionNoSchema, erpDailyDimaSchema, erpDailyNoSchema}
import it.eng.au.ERP.schema.rcu.{rcuPodPSchema, rcuPodDistrPSchema, RcuAziendaPSchema}
import it.eng.au.ERP.schema.rcus.rcusPodDistrSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object ProfilazioneCurveNOTransformation {

  private val DatePatternCompact = "yyyyMMdd"
  private val DatePatternDash = "yyyy-MM-dd"
  private val DatePatternSlash = "dd/MM/yyyy"

  private def parseDate(colName: String): Column = {
    coalesce(
      to_date(col(colName), DatePatternCompact),
      to_date(col(colName), DatePatternDash),
      to_date(col(colName), DatePatternSlash)
    )
  }

  def buildDailyProfiles(
      consumptionDf: DataFrame,
      dailyDimaDf: DataFrame,
      podAnagDf: DataFrame,
      podDistrPDf: DataFrame,
      podDistrHistDf: DataFrame,
      aziendaDf: DataFrame,
      podExcluded: List[String],
      executionId: Long
  )(implicit spark: SparkSession): DataFrame = {
    val executionIdValue = executionId.toString

    val exclusionFilterDf =
      if (podExcluded.nonEmpty)
        consumptionDf.filter(!col(erpConsumptionNoSchema.pod).isin(podExcluded: _*))
      else
        consumptionDf

    val segmentsForProfiling = exclusionFilterDf
      .filter(col(erpConsumptionNoSchema.executionid) === executionIdValue)
      .filter(col(erpConsumptionNoSchema.stato) === lit("OK"))
      // Punto 3: Scarta segmenti con ENTRAMBE le sponde = SMIS_MN
      .filter(!(
        upper(trim(col(erpConsumptionNoSchema.tipo_flusso_sx))) === "SMIS_MN" &&
        upper(trim(col(erpConsumptionNoSchema.tipo_flusso_dx))) === "SMIS_MN"
      ))
      .withColumn("data_sx_date", parseDate(erpConsumptionNoSchema.data_misura_sx))
      .withColumn("data_dx_date", parseDate(erpConsumptionNoSchema.data_misura_dx))
      .filter(col("data_sx_date").isNotNull && col("data_dx_date").isNotNull)
      .withColumn(
        "profilo_tipo",
        when(upper(trim(col(erpConsumptionNoSchema.trattamento))) === "F", lit("F"))
          .when(upper(trim(col(erpConsumptionNoSchema.trattamento))) === "M", lit("M"))
          .when(
            upper(trim(col(erpConsumptionNoSchema.trattamento))) === "C" &&
              (col(erpConsumptionNoSchema.c_eaf1).isNotNull ||
                col(erpConsumptionNoSchema.c_eaf2).isNotNull ||
                col(erpConsumptionNoSchema.c_eaf3).isNotNull),
            lit("F")
          )
          .otherwise(lit("M"))
      )
      // Per SMIS_A lo shift a +1 è già applicato a monte (validated);
      // per gli altri flussi manteniamo (left, right] quindi +1 sul giorno iniziale.
      .withColumn(
        "start_giorno",
        when(upper(trim(col(erpConsumptionNoSchema.tipo_flusso_sx))) === lit("SMIS_A"),
          col("data_sx_date")
        ).otherwise(date_add(col("data_sx_date"), 1))
      )
      .withColumn("end_giorno", col("data_dx_date"))
      .filter(col("end_giorno") >= col("start_giorno"))

    // Calculate total quarters for the entire period BEFORE explode
    val dailyDimaPrepared = dailyDimaDf
      .withColumn(
        "giorno_dima",
        coalesce(
          to_date(col(erpDailyDimaSchema.data), DatePatternSlash),
          to_date(col(erpDailyDimaSchema.data), DatePatternDash),
          to_date(col(erpDailyDimaSchema.data), DatePatternCompact)
        )
      )
      .filter(col("giorno_dima").isNotNull)
      .withColumn("tipo_normalizzato", upper(trim(col(erpDailyDimaSchema.tipo))))

    // Join segments with daily_dima to sum total quarters per period
    val segmentsWithTotals = segmentsForProfiling
      .join(
        dailyDimaPrepared,
        col("giorno_dima") >= col("start_giorno") &&
          col("giorno_dima") <= col("end_giorno") &&
          col("profilo_tipo") === col("tipo_normalizzato"),
        "left"
      )
      .groupBy(
        col(erpConsumptionNoSchema.pod),
        col(erpConsumptionNoSchema.executionid),
        col(erpConsumptionNoSchema.data_misura_sx),
        col(erpConsumptionNoSchema.data_misura_dx),
        col(erpConsumptionNoSchema.trattamento),
        col(erpConsumptionNoSchema.c_eam),
        col(erpConsumptionNoSchema.c_eaf1),
        col(erpConsumptionNoSchema.c_eaf2),
        col(erpConsumptionNoSchema.c_eaf3),
        col("data_sx_date"),
        col("data_dx_date"),
        col("profilo_tipo"),
        col("start_giorno"),
        col("end_giorno")
      )
      .agg(
        sum(coalesce(col(erpDailyDimaSchema.numq_f0).cast(DoubleType), lit(0d))).alias("total_numq_f0"),
        sum(coalesce(col(erpDailyDimaSchema.numq_f1).cast(DoubleType), lit(0d))).alias("total_numq_f1"),
        sum(coalesce(col(erpDailyDimaSchema.numq_f2).cast(DoubleType), lit(0d))).alias("total_numq_f2"),
        sum(coalesce(col(erpDailyDimaSchema.numq_f3).cast(DoubleType), lit(0d))).alias("total_numq_f3")
      )
      .withColumn(erpDailyNoSchema.giorno, explode(sequence(col("start_giorno"), col("end_giorno"))))

    val podArea = podAnagDf
      .select(
        trim(col(rcuPodPSchema.t_codice_pod)).alias("pod_key"),
        col(rcuPodPSchema.n_id_pod).alias("n_id_pod_key"),
        trim(col(rcuPodPSchema.t_area_rif)).alias("area_value")
      )
      .dropDuplicates("pod_key")

    // Prepare UNION of RCU_POD_DISTR_P and RCUS_POD_DISTR (b_valido='N')
    val podDistrFromRcuP = podDistrPDf
      .select(
        col(rcuPodDistrPSchema.n_id_pod).alias("n_id_pod_union"),
        col(rcuPodDistrPSchema.n_id_distr).alias("n_id_distr_union"),
        coalesce(
          to_date(col(rcuPodDistrPSchema.d_inizio), "yyyy-MM-dd"),
          lit("1000-01-01").cast("date")
        ).alias("d_inizio_union"),
        coalesce(
          to_date(col(rcuPodDistrPSchema.d_fine), "yyyy-MM-dd"),
          lit("3000-01-01").cast("date")
        ).alias("d_fine_union")
      )

    val podDistrFromRcusP = podDistrHistDf
      .filter(col(rcusPodDistrSchema.b_valido) === "N")
      .filter(
        coalesce(
          to_date(col(rcusPodDistrSchema.d_inizio), "yyyy-MM-dd"),
          lit("1000-01-01").cast("date")
        ) <= coalesce(
          to_date(col(rcusPodDistrSchema.d_fine), "yyyy-MM-dd"),
          lit("3000-01-01").cast("date")
        )
      )
      .select(
        col(rcusPodDistrSchema.n_id_pod).alias("n_id_pod_union"),
        col(rcusPodDistrSchema.n_id_distr).alias("n_id_distr_union"),
        coalesce(
          to_date(col(rcusPodDistrSchema.d_inizio), "yyyy-MM-dd"),
          lit("1000-01-01").cast("date")
        ).alias("d_inizio_union"),
        coalesce(
          to_date(col(rcusPodDistrSchema.d_fine), "yyyy-MM-dd"),
          lit("3000-01-01").cast("date")
        ).alias("d_fine_union")
      )

    // UNION of both sources
    val podDistrUnion = podDistrFromRcuP.union(podDistrFromRcusP)

    val podWithIdPod = podAnagDf
      .select(
        trim(col(rcuPodPSchema.t_codice_pod)).alias("pod_rcu"),
        col(rcuPodPSchema.n_id_pod).alias("n_id_pod_rcu")
      )

    val aziendaPrepared = aziendaDf
      .select(
        col(RcuAziendaPSchema.n_id_azienda).alias("n_id_azienda_az"),
        trim(col(RcuAziendaPSchema.t_piva)).alias("piva_distr_value"),
        trim(col(RcuAziendaPSchema.t_rag_soc)).alias("rag_soc_distr_value")
      )

    val useF = col("profilo_tipo") === lit("F")
    val useM = col("profilo_tipo") === lit("M")

    // Now join with daily_dima again to get the daily distribution within the period
    val joined = segmentsWithTotals
      .join(
        dailyDimaPrepared,
        segmentsWithTotals.col(erpDailyNoSchema.giorno) === dailyDimaPrepared.col("giorno_dima") &&
          segmentsWithTotals.col("profilo_tipo") === dailyDimaPrepared.col("tipo_normalizzato"),
        "left"
      )
      .withColumn("numq_f0_d", coalesce(col(erpDailyDimaSchema.numq_f0).cast(DoubleType), lit(0d)))
      .withColumn("numq_f1_d", coalesce(col(erpDailyDimaSchema.numq_f1).cast(DoubleType), lit(0d)))
      .withColumn("numq_f2_d", coalesce(col(erpDailyDimaSchema.numq_f2).cast(DoubleType), lit(0d)))
      .withColumn("numq_f3_d", coalesce(col(erpDailyDimaSchema.numq_f3).cast(DoubleType), lit(0d)))

    // Use TOTAL quarters for the period, not daily quarters
    val valueF0 = when(useM && col("total_numq_f0") > lit(0d), col(erpConsumptionNoSchema.c_eam) / col("total_numq_f0"))
      .otherwise(lit(0d))
    val valueF1 = when(useF && col("total_numq_f1") > lit(0d), col(erpConsumptionNoSchema.c_eaf1) / col("total_numq_f1"))
      .otherwise(lit(0d))
    val valueF2 = when(useF && col("total_numq_f2") > lit(0d), col(erpConsumptionNoSchema.c_eaf2) / col("total_numq_f2"))
      .otherwise(lit(0d))
    val valueF3 = when(useF && col("total_numq_f3") > lit(0d), col(erpConsumptionNoSchema.c_eaf3) / col("total_numq_f3"))
      .otherwise(lit(0d))

    def quarterValue(columnName: String): Column = {
      val code = upper(trim(col(columnName)))
      when(code === "F0", valueF0)
        .when(code === "F1", valueF1)
        .when(code === "F2", valueF2)
        .when(code === "F3", valueF3)
        .otherwise(lit(0d))
    }

    val baseColumns = joined
      .withColumn(erpDailyNoSchema.pod, trim(col(erpConsumptionNoSchema.pod)))
      .withColumn(erpDailyNoSchema.area, lit(null: String))
      .withColumn(erpDailyNoSchema.piva_distr, lit(null: String))
      .withColumn(erpDailyNoSchema.rag_soc_distr, lit(null: String))
  .withColumn(erpDailyNoSchema.anno, year(col(erpDailyNoSchema.giorno)).cast("string"))  // STRING
  .withColumn(erpDailyNoSchema.mese, lpad(month(col(erpDailyNoSchema.giorno)).cast("string"), 2, "0")) // STRING, zero-padded
      .withColumn(erpDailyNoSchema.executionid, lit(executionIdValue))

    val withArea = baseColumns
      .join(podArea, baseColumns.col(erpDailyNoSchema.pod) === podArea.col("pod_key"), "left")
      .withColumn(erpDailyNoSchema.area, coalesce(col("area_value"), col(erpDailyNoSchema.area)))

    // Join with RCU tables to get distributor info, filtering by date
    // Use UNION of RCU_POD_DISTR_P and RCUS_POD_DISTR (like SQL query)
    val withDistributorInfo = withArea
      .join(
        podWithIdPod,
        withArea.col(erpDailyNoSchema.pod) === podWithIdPod.col("pod_rcu"),
        "left"
      )
      .join(
        podDistrUnion,
        col("n_id_pod_rcu") === col("n_id_pod_union") &&
          col(erpDailyNoSchema.giorno) >= col("d_inizio_union") &&
          col(erpDailyNoSchema.giorno) <= col("d_fine_union"),
        "left"
      )
      .join(
        aziendaPrepared,
        col("n_id_distr_union") === col("n_id_azienda_az"),
        "left"
      )
      .withColumn(erpDailyNoSchema.piva_distr, coalesce(col("piva_distr_value"), col(erpDailyNoSchema.piva_distr)))
      .withColumn(erpDailyNoSchema.rag_soc_distr, coalesce(col("rag_soc_distr_value"), col(erpDailyNoSchema.rag_soc_distr)))

    val profiled = (1 to 100).foldLeft(withDistributorInfo) { (df, i) =>
      val qhCol = s"qh$i"  // Column name in erpDailyDimaSchema (input)
      val qCol = s"q$i"    // Column name in erpDailyNoSchema (output)
      df.withColumn(qCol, quarterValue(qhCol))
    }

    profiled.selectExpr(erpDailyNoSchema.getValues: _*)
  }
}
