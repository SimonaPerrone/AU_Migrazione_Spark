package it.eng.au.ERP.trasformations.NO

import it.eng.au.ERP.schema.erp.{erpConsumptionNoSchema, erpValidatedMisNoSchema}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

object CalcoloSegmentIConsumoNOTrasformation {

  private val RankColumn = "rank"
  private val CountColumn = "row_count"

  private val SX = "_SX"
  private val DX = "_DX"

  private val ERR_MIS  = "ERR_MIS"
  private val ERR_SEG =  "ERR_SEG"
  private val ERR_T = "ERR_T"
  private val ERR_CONS = "ERR_CONS"
  private val OK ="OK"

  private val windowErrorSegmentoSinistro  = Seq("SMIS_SMN","DS","DS2G")
  private val windowErrorSegmentoDestro    = Seq("SMIS_MN","AV","AV2G")

  private val columnsToShift: Seq[(String, String)] = Seq(
    erpValidatedMisNoSchema.data_misura.toString -> (erpValidatedMisNoSchema.data_misura.toString + DX),
    erpValidatedMisNoSchema.tipo_flusso.toString -> (erpValidatedMisNoSchema.tipo_flusso.toString + DX),
    erpValidatedMisNoSchema.nomefile.toString -> (erpValidatedMisNoSchema.nomefile.toString + DX),
    erpValidatedMisNoSchema.eam.toString -> (erpValidatedMisNoSchema.eam.toString + DX),
    erpValidatedMisNoSchema.eaf1.toString -> (erpValidatedMisNoSchema.eaf1.toString + DX),
    erpValidatedMisNoSchema.eaf2.toString -> (erpValidatedMisNoSchema.eaf2.toString + DX),
    erpValidatedMisNoSchema.eaf3.toString -> (erpValidatedMisNoSchema.eaf3.toString + DX),
    erpValidatedMisNoSchema.coeff_perdita.toString -> (erpValidatedMisNoSchema.coeff_perdita.toString + DX),
    erpValidatedMisNoSchema.k.toString -> (erpValidatedMisNoSchema.k.toString + DX),
    erpValidatedMisNoSchema.tipo_dato_s.toString -> (erpValidatedMisNoSchema.tipo_dato_s.toString + DX),
    erpValidatedMisNoSchema.tipo_dato_e.toString -> (erpValidatedMisNoSchema.tipo_dato_e.toString + DX),
    erpValidatedMisNoSchema.time_stamp.toString -> (erpValidatedMisNoSchema.time_stamp.toString + DX)
  )

  private def toDouble(col: Column): Column =
    regexp_replace(col.cast(StringType), ",", ".").cast(DoubleType)

  private def normalize(value: Column, coeff: Column, k: Column): Column =
    toDouble(value) * toDouble(coeff) * toDouble(k)

  private def allNotNull(columns: Column*): Column =
    columns.map(_.isNotNull).reduce(_.and(_))

  private def anyNegative(columns: Column*): Column =
    columns.map(_ < lit(0d)).reduce(_.or(_))

  private def baseFilter(df: DataFrame, podExcluded: List[String]): DataFrame =
    if (podExcluded.nonEmpty)
      df.filter(!col(erpValidatedMisNoSchema.pod.toString).isin(podExcluded: _*))
    else
      df

  def buildConsumptionSegments(dfErpValidatedMisNO: DataFrame,
                               annomese: Option[String],
                               executionId: Long,
                               podExcluded: List[String])
                              (implicit spark: SparkSession): DataFrame = {

    val filtered = baseFilter(dfErpValidatedMisNO, podExcluded)

    // Tie-breaker specifico per SMIS: se data_misura e time_stamp sono uguali,
    // ordina prima SMIS_A e poi SMIS_B (gli altri dopo).
    val tieBreakSmis = when(upper(trim(col(erpValidatedMisNoSchema.tipo_flusso.toString))) === lit("SMIS_A"), lit(0))
      .when(upper(trim(col(erpValidatedMisNoSchema.tipo_flusso.toString))) === lit("SMIS_B"), lit(1))
      .otherwise(lit(2))

    val orderWindow = Window
      .partitionBy(col(erpValidatedMisNoSchema.pod.toString))
      .orderBy(
        col(erpValidatedMisNoSchema.data_misura.toString).asc_nulls_last,
        col(erpValidatedMisNoSchema.time_stamp.toString).asc_nulls_last,
        tieBreakSmis.asc
      )

    val countWindow = Window.partitionBy(col(erpValidatedMisNoSchema.pod.toString))

    val ranked = columnsToShift.foldLeft(
      filtered
        .withColumn(RankColumn, row_number().over(orderWindow))
        .withColumn(CountColumn, count(lit(1)).over(countWindow))
    ){ case (tmp, (columnName, alias)) =>
      tmp.withColumn(alias, lead(col(columnName), 1).over(orderWindow))
    }

    val annomeseValue = annomese.getOrElse(null)
    val executionIdValue = executionId.toString

    val segmentsBase = ranked
      .filter(col(CountColumn) > 1 && col(erpValidatedMisNoSchema.data_misura.toString + DX).isNotNull)
      .select(
        col(erpValidatedMisNoSchema.pod.toString).alias(erpConsumptionNoSchema.pod.toString),
        upper(trim(col(erpValidatedMisNoSchema.trattamento.toString))).alias("trattamento_norm"),
        col(erpValidatedMisNoSchema.trattamento.toString).alias(erpConsumptionNoSchema.trattamento.toString),
        col(erpValidatedMisNoSchema.data_misura.toString).alias(erpConsumptionNoSchema.data_misura_sx.toString),
        col(erpValidatedMisNoSchema.tipo_flusso.toString).alias(erpConsumptionNoSchema.tipo_flusso_sx.toString),
        col(erpValidatedMisNoSchema.nomefile.toString).alias(erpConsumptionNoSchema.nomefile_sx.toString),
        col(erpValidatedMisNoSchema.eam.toString).alias("eam" + SX),
        col(erpValidatedMisNoSchema.eaf1.toString).alias("eaf1" + SX),
        col(erpValidatedMisNoSchema.eaf2.toString).alias("eaf2" + SX),
        col(erpValidatedMisNoSchema.eaf3.toString).alias("eaf3" + SX),
        col(erpValidatedMisNoSchema.coeff_perdita.toString).alias("coeff" + SX),
        col(erpValidatedMisNoSchema.k.toString).alias("k" + SX),
        col(erpValidatedMisNoSchema.data_misura.toString + DX).alias(erpConsumptionNoSchema.data_misura_dx.toString),
        col(erpValidatedMisNoSchema.tipo_flusso.toString + DX).alias(erpConsumptionNoSchema.tipo_flusso_dx.toString),
        col(erpValidatedMisNoSchema.nomefile.toString + DX).alias(erpConsumptionNoSchema.nomefile_dx.toString),
        col(erpValidatedMisNoSchema.eam.toString + DX).alias("eam" + DX),
        col(erpValidatedMisNoSchema.eaf1.toString + DX).alias("eaf1" + DX),
        col(erpValidatedMisNoSchema.eaf2.toString + DX).alias("eaf2" + DX),
        col(erpValidatedMisNoSchema.eaf3.toString + DX).alias("eaf3" + DX),
        col(erpValidatedMisNoSchema.coeff_perdita.toString + DX).alias("coeff" + DX),
        col(erpValidatedMisNoSchema.k.toString + DX).alias("k" + DX)
      )

    val segmentsNormalized = segmentsBase
      .withColumn(erpConsumptionNoSchema.eam_sx.toString, normalize(col("eam" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eaf1_sx.toString, normalize(col("eaf1" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eaf2_sx.toString, normalize(col("eaf2" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eaf3_sx.toString, normalize(col("eaf3" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eam_dx.toString, normalize(col("eam" + DX), col("coeff" + DX), col("k" + DX)))
      .withColumn(erpConsumptionNoSchema.eaf1_dx.toString, normalize(col("eaf1" + DX), col("coeff" + DX), col("k" + DX)))
      .withColumn(erpConsumptionNoSchema.eaf2_dx.toString, normalize(col("eaf2" + DX), col("coeff" + DX), col("k" + DX)))
      .withColumn(erpConsumptionNoSchema.eaf3_dx.toString, normalize(col("eaf3" + DX), col("coeff" + DX), col("k" + DX)))
      .withColumn(erpConsumptionNoSchema.c_eam.toString,
        when(col(erpConsumptionNoSchema.eam_sx.toString).isNotNull && col(erpConsumptionNoSchema.eam_dx.toString).isNotNull,
          col(erpConsumptionNoSchema.eam_dx.toString) - col(erpConsumptionNoSchema.eam_sx.toString))
      )
      .withColumn(erpConsumptionNoSchema.c_eaf1.toString,
        when(col(erpConsumptionNoSchema.eaf1_sx.toString).isNotNull && col(erpConsumptionNoSchema.eaf1_dx.toString).isNotNull,
          col(erpConsumptionNoSchema.eaf1_dx.toString) - col(erpConsumptionNoSchema.eaf1_sx.toString))
      )
      .withColumn(erpConsumptionNoSchema.c_eaf2.toString,
        when(col(erpConsumptionNoSchema.eaf2_sx.toString).isNotNull && col(erpConsumptionNoSchema.eaf2_dx.toString).isNotNull,
          col(erpConsumptionNoSchema.eaf2_dx.toString) - col(erpConsumptionNoSchema.eaf2_sx.toString))
      )
      .withColumn(erpConsumptionNoSchema.c_eaf3.toString,
        when(col(erpConsumptionNoSchema.eaf3_sx.toString).isNotNull && col(erpConsumptionNoSchema.eaf3_dx.toString).isNotNull,
          col(erpConsumptionNoSchema.eaf3_dx.toString) - col(erpConsumptionNoSchema.eaf3_sx.toString))
      )

    val segmentsWithFlags = segmentsNormalized
      .withColumn("has_eam", allNotNull(col(erpConsumptionNoSchema.eam_sx.toString), col(erpConsumptionNoSchema.eam_dx.toString)))
      .withColumn("has_eaf",
        allNotNull(
          col(erpConsumptionNoSchema.eaf1_sx.toString), col(erpConsumptionNoSchema.eaf1_dx.toString),
          col(erpConsumptionNoSchema.eaf2_sx.toString), col(erpConsumptionNoSchema.eaf2_dx.toString),
          col(erpConsumptionNoSchema.eaf3_sx.toString), col(erpConsumptionNoSchema.eaf3_dx.toString)
        )
      )
      .withColumn("invalid_flow",
        col(erpConsumptionNoSchema.tipo_flusso_sx.toString).isin(windowErrorSegmentoSinistro: _*).
          or(col(erpConsumptionNoSchema.tipo_flusso_dx.toString).isin(windowErrorSegmentoDestro: _*))
      )
      .withColumn("negative_eam", col(erpConsumptionNoSchema.c_eam.toString) < lit(0d))
      .withColumn("negative_eaf", anyNegative(
        col(erpConsumptionNoSchema.c_eaf1.toString),
        col(erpConsumptionNoSchema.c_eaf2.toString),
        col(erpConsumptionNoSchema.c_eaf3.toString)
      ))

    val trattamentoNormCol = col("trattamento_norm")
    val isM = trattamentoNormCol === lit("M")
    val isF = trattamentoNormCol === lit("F")
    val isC = trattamentoNormCol === lit("C")
    val hasEamCol = col("has_eam")
    val hasEafCol = col("has_eaf")
    val missingEamCol = not(hasEamCol)
    val missingEafCol = not(hasEafCol)
    val useEamConsumption = isM.or(isC.and(missingEafCol).and(hasEamCol))
    val useEafConsumption = isF.or(isC.and(hasEafCol))

    val segmentsWithStatus = segmentsWithFlags
      .withColumn(erpConsumptionNoSchema.annomese.toString, lit(annomeseValue))
      .withColumn(erpConsumptionNoSchema.executionid.toString, lit(executionIdValue))
      .withColumn(erpConsumptionNoSchema.stato.toString,
        when(col("invalid_flow"), lit(ERR_MIS))
          .when(isM.and(missingEamCol), lit(ERR_T))
          .when(isF.and(missingEafCol), lit(ERR_T))
          .when(isC.and(missingEafCol).and(missingEamCol), lit(ERR_T))
          .when(useEamConsumption.and(col("negative_eam")), lit(ERR_CONS))
          .when(useEafConsumption.and(col("negative_eaf")), lit(ERR_CONS))
          .otherwise(lit(OK))
      )

    val errSegBase = ranked
      .filter(col(CountColumn) === 1)
      .select(
        col(erpValidatedMisNoSchema.pod.toString).alias(erpConsumptionNoSchema.pod.toString),
        upper(trim(col(erpValidatedMisNoSchema.trattamento.toString))).alias("trattamento_norm"),
        col(erpValidatedMisNoSchema.trattamento.toString).alias(erpConsumptionNoSchema.trattamento.toString),
        col(erpValidatedMisNoSchema.data_misura.toString).alias(erpConsumptionNoSchema.data_misura_sx.toString),
        col(erpValidatedMisNoSchema.tipo_flusso.toString).alias(erpConsumptionNoSchema.tipo_flusso_sx.toString),
        col(erpValidatedMisNoSchema.nomefile.toString).alias(erpConsumptionNoSchema.nomefile_sx.toString),
        col(erpValidatedMisNoSchema.eam.toString).alias("eam" + SX),
        col(erpValidatedMisNoSchema.eaf1.toString).alias("eaf1" + SX),
        col(erpValidatedMisNoSchema.eaf2.toString).alias("eaf2" + SX),
        col(erpValidatedMisNoSchema.eaf3.toString).alias("eaf3" + SX),
        col(erpValidatedMisNoSchema.coeff_perdita.toString).alias("coeff" + SX),
        col(erpValidatedMisNoSchema.k.toString).alias("k" + SX)
      )

    val errSegNormalized = errSegBase
      .withColumn(erpConsumptionNoSchema.eam_sx.toString, normalize(col("eam" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eaf1_sx.toString, normalize(col("eaf1" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eaf2_sx.toString, normalize(col("eaf2" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eaf3_sx.toString, normalize(col("eaf3" + SX), col("coeff" + SX), col("k" + SX)))
      .withColumn(erpConsumptionNoSchema.eam_dx.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.eaf1_dx.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.eaf2_dx.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.eaf3_dx.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.data_misura_dx.toString, lit(null).cast(StringType))
      .withColumn(erpConsumptionNoSchema.tipo_flusso_dx.toString, lit(null).cast(StringType))
      .withColumn(erpConsumptionNoSchema.nomefile_dx.toString, lit(null).cast(StringType))
      .withColumn(erpConsumptionNoSchema.c_eam.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.c_eaf1.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.c_eaf2.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.c_eaf3.toString, lit(null).cast(DoubleType))
      .withColumn(erpConsumptionNoSchema.annomese.toString, lit(annomeseValue))
      .withColumn(erpConsumptionNoSchema.executionid.toString, lit(executionIdValue))
      .withColumn(erpConsumptionNoSchema.stato.toString, lit(ERR_SEG))

    val segmentsSelected = segmentsWithStatus
      .drop("trattamento_norm", "has_eam", "has_eaf", "invalid_flow", "negative_eam", "negative_eaf")
      .selectExpr(erpConsumptionNoSchema.getValues: _*)

    val errSegSelected = errSegNormalized
      .drop("trattamento_norm")
      .selectExpr(erpConsumptionNoSchema.getValues: _*)

    segmentsSelected.unionByName(errSegSelected)
  }
}
