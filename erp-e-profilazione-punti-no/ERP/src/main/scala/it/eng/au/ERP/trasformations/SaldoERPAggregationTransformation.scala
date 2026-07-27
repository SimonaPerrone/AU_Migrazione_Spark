package it.eng.au.ERP.trasformations

import it.eng.au.ERP.schema.erp.{erpAggregatoINTSchema, erpAggregatoOSchema, erpAggregatoNoSchema, erpAggregatoIPOSchema, erpAggregatoSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaldoERPAggregationTransformation {

  private val qCols: Seq[String] = (1 to 100).map(i => s"q$i")

  private def latestForMonth(df: DataFrame, anno: Int, mese: Int, execCol: String)(implicit spark: SparkSession): DataFrame = {
    val yearStr = anno.toString
    val monthStr = f"$mese%02d"
    val filtered = df
      .filter(col("anno") === lit(yearStr))
      .filter(col("mese") === lit(monthStr))
    if (filtered.head(1).isEmpty) return filtered
    val maxExec = filtered.agg(max(col(execCol).cast("long"))).first().get(0)
    filtered.filter(col(execCol).cast("long") === lit(maxExec))
  }

  def computeSaldo(
      dfInt: DataFrame,
      dfO: DataFrame,
      dfNo: DataFrame,
      dfIp: DataFrame,
      anno: Int,
      mese: Int,
      executionId: Long
    )(implicit spark: SparkSession): DataFrame = {

    val intLatest = latestForMonth(dfInt, anno, mese, erpAggregatoINTSchema.executionid.toString)
      .select(
        (Seq(
          col(erpAggregatoINTSchema.giorno.toString).alias(erpAggregatoSchema.giorno.toString),
          col(erpAggregatoINTSchema.area.toString).alias(erpAggregatoSchema.area.toString),
          col(erpAggregatoINTSchema.piva_distr.toString).alias(erpAggregatoSchema.piva_distr.toString),
          col(erpAggregatoINTSchema.rag_soc_distr.toString).alias(erpAggregatoSchema.rag_soc_distr.toString)
        ) ++ qCols.map(c => col(c).alias(c))): _*
      )

    val oLatest = latestForMonth(dfO, anno, mese, erpAggregatoOSchema.executionid.toString)
      .select(
        (Seq(
          col(erpAggregatoOSchema.giorno.toString).alias("giorno"),
          col(erpAggregatoOSchema.area.toString).alias("area"),
          col(erpAggregatoOSchema.piva_distr.toString).alias("piva_distr"),
          col(erpAggregatoOSchema.rag_soc_distr.toString).alias("rag_soc_distr")
        ) ++ qCols.map(c => col(c).alias(s"o_$c"))): _*
      )

    val noLatest = latestForMonth(dfNo, anno, mese, erpAggregatoNoSchema.executionid.toString)
      .select(
        (Seq(
          col(erpAggregatoNoSchema.giorno.toString).alias("giorno"),
          col(erpAggregatoNoSchema.area.toString).alias("area"),
          col(erpAggregatoNoSchema.piva_distr.toString).alias("piva_distr"),
          col(erpAggregatoNoSchema.rag_soc_distr.toString).alias("rag_soc_distr")
        ) ++ qCols.map(c => col(c).alias(s"no_$c"))): _*
      )

    val ipLatest = latestForMonth(dfIp, anno, mese, erpAggregatoIPOSchema.executionid.toString)
      .select(
        (Seq(
          col(erpAggregatoIPOSchema.giorno.toString).alias("giorno"),
          col(erpAggregatoIPOSchema.area.toString).alias("area"),
          col(erpAggregatoIPOSchema.piva_distr.toString).alias("piva_distr"),
          col(erpAggregatoIPOSchema.rag_soc_distr.toString).alias("rag_soc_distr")
        ) ++ qCols.map(c => col(c).alias(s"ip_$c"))): _*
      )

    val keyCols = Seq("giorno","area","piva_distr","rag_soc_distr")

    val allKeys = intLatest.select(keyCols.map(col): _*)
      .union(oLatest.select(keyCols.map(col): _*))
      .union(noLatest.select(keyCols.map(col): _*))
      .union(ipLatest.select(keyCols.map(col): _*))
      .distinct()

    val joined = allKeys
      .join(intLatest, keyCols, "left")
      .join(oLatest,   keyCols, "left")
      .join(noLatest,  keyCols, "left")
      .join(ipLatest,  keyCols, "left")

    val saldoCols = qCols.map { c =>
      (
        coalesce(col(c), lit(0.0))
          - coalesce(col(s"o_$c"), lit(0.0))
          - coalesce(col(s"no_$c"), lit(0.0))
          - coalesce(col(s"ip_$c"), lit(0.0))
      ).alias(c)
    }

    val yearStr = anno.toString
    val monthStr = f"$mese%02d"

    val out = joined
      .select(
        (Seq(
          col("giorno").alias(erpAggregatoSchema.giorno.toString),
          col("area").alias(erpAggregatoSchema.area.toString),
          col("piva_distr").alias(erpAggregatoSchema.piva_distr.toString),
          col("rag_soc_distr").alias(erpAggregatoSchema.rag_soc_distr.toString)
        ) ++ saldoCols): _*
      )
      .withColumn(erpAggregatoSchema.anno.toString, lit(yearStr))
      .withColumn(erpAggregatoSchema.mese.toString, lit(monthStr))
      .withColumn(erpAggregatoSchema.executionid.toString, lit(executionId.toString))

    out
  }
}
