package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.model.agg.ErrorEnum
import it.eng.au.aggiustamentoGas.schema.agg.DailyConsumptionAGGSBGSchema
import it.eng.au.aggiustamentoGas.schema.settlegas.TabProfiliGiorniStdPercSchema
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.FLOW_DATE_FORMAT
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, concat_ws, date_format, lit, md5, sum, to_timestamp, when}
import org.apache.spark.sql.types.DoubleType

class PprofInfoController extends Serializable {

  def get(dailyC: DataFrame, tabProfStdPercBefore: DataFrame, tabProfStdPercAfter: DataFrame, tabProfStdOutOfMonthPre: DataFrame, tabProfStdOutOfMonthPost: DataFrame): DataFrame = {

    val colsToJoinOutOfMonthPre = Seq(DailyConsumptionAGGSBGSchema.pdr.toString, DailyConsumptionAGGSBGSchema.codProfStd.toString, DailyConsumptionAGGSBGSchema.idRegClim.toString, "startDateF2", "endDateF2")
    val colsToJoinOutOfMonthPost = Seq(DailyConsumptionAGGSBGSchema.pdr.toString, DailyConsumptionAGGSBGSchema.codProfStd.toString, DailyConsumptionAGGSBGSchema.idRegClim.toString, DailyConsumptionAGGSBGSchema.codRemi.toString, "startDateF2", "endDateF2")

    dailyC
      .join(tabProfStdPercBefore.drop(DailyConsumptionAGGSBGSchema.codRemi.toString)
        , Seq(DailyConsumptionAGGSBGSchema.date.toString, DailyConsumptionAGGSBGSchema.codProfStd.toString, DailyConsumptionAGGSBGSchema.idRegClim.toString)
        , "left")
      .join(tabProfStdPercAfter
        , Seq(DailyConsumptionAGGSBGSchema.date.toString, DailyConsumptionAGGSBGSchema.codProfStd.toString, DailyConsumptionAGGSBGSchema.idRegClim.toString, DailyConsumptionAGGSBGSchema.codRemi.toString)
        , "left")
      .join(tabProfStdOutOfMonthPre.drop(DailyConsumptionAGGSBGSchema.codRemi.toString), colsToJoinOutOfMonthPre, "left")
      .join(tabProfStdOutOfMonthPost, colsToJoinOutOfMonthPost, "left")
      .withColumn(
        "residuo",
        when(col("residuo_pre").isNotNull && col("residuo_post").isNotNull,
          col("residuo_pre") + col("residuo_post"))
          .when(col("residuo_pre").isNotNull && col("residuo_post").isNull,
            col("residuo_pre"))
          .when(col("residuo_pre").isNull && col("residuo_post").isNotNull,
            col("residuo_post"))
          .otherwise(lit(null))
      )
      .withColumn(
        DailyConsumptionAGGSBGSchema.pprof,
        when(
          col(DailyConsumptionAGGSBGSchema.errorCode).isin(2, 3, 4), //13
          lit(null)
        ).otherwise(
          coalesce(
            col(TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_1"),
            col(TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_2")
          )
        )
      )
  }

  def getOutMonth(
                   dailyC: DataFrame,
                   tabProfStdPercBefore: DataFrame,
                   tabProfStdPercAfter: DataFrame
                 ): (DataFrame, DataFrame) = {
    // -----------------------------
    // 1. Normalizzo le date subito
    // -----------------------------
    val dailyNorm = dailyC
      .withColumn("startDateF2", to_timestamp(col("startDateF2"), FLOW_DATE_FORMAT))
      .withColumn("endDateF2",   to_timestamp(col("endDateF2"),   FLOW_DATE_FORMAT))
      .filter(col("startDateF2").isNotNull)
      .select(
        col(DailyConsumptionAGGSBGSchema.pdr), col(DailyConsumptionAGGSBGSchema.codProfStd),
        col(DailyConsumptionAGGSBGSchema.idRegClim), col(DailyConsumptionAGGSBGSchema.codRemi),
        col("startDateF2"), col("endDateF2"))

    // --------------------------------------------------------
    // 2. JOIN PRE-REMI (tabProfStdPercBefore senza codRemi)
    // --------------------------------------------------------
    val preJoined = dailyNorm
      .join(
        tabProfStdPercBefore.drop(DailyConsumptionAGGSBGSchema.codRemi),
        Seq(DailyConsumptionAGGSBGSchema.codProfStd.toString, DailyConsumptionAGGSBGSchema.idRegClim.toString),
        "left"
      )
      .filter(col(DailyConsumptionAGGSBGSchema.date.toString ++ "_3").between(col("startDateF2"), col("endDateF2")))
      .distinct

    val residuoPre = preJoined
      .groupBy(col(DailyConsumptionAGGSBGSchema.pdr), col(DailyConsumptionAGGSBGSchema.codProfStd), col(DailyConsumptionAGGSBGSchema.idRegClim), col("startDateF2"), col("endDateF2"))
      .agg(sum(col(TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_3")).alias("residuo_pre"))
      .withColumn("startDateF2", date_format(col("startDateF2"), FLOW_DATE_FORMAT))
      .withColumn("endDateF2", date_format(col("endDateF2"), FLOW_DATE_FORMAT))
      .filter(col("residuo_pre") > 0.0)

    // --------------------------------------------------------
    // 3. JOIN POST-REMI
    // --------------------------------------------------------

    val postJoined = dailyNorm
      .join(
        tabProfStdPercAfter,
        Seq(DailyConsumptionAGGSBGSchema.codProfStd.toString, DailyConsumptionAGGSBGSchema.idRegClim.toString, DailyConsumptionAGGSBGSchema.codRemi.toString),
        "left"
      )
      .filter(col(DailyConsumptionAGGSBGSchema.date.toString ++ "_4").between(col("startDateF2"), col("endDateF2")))
      .distinct

    val residuoPost = postJoined
      .groupBy(col(DailyConsumptionAGGSBGSchema.pdr), col(DailyConsumptionAGGSBGSchema.codProfStd), col(DailyConsumptionAGGSBGSchema.idRegClim), col(DailyConsumptionAGGSBGSchema.codRemi), col("startDateF2"), col("endDateF2"))
      .agg(sum(col(TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_4")).alias("residuo_post"))
      .withColumn("startDateF2", date_format(col("startDateF2"), FLOW_DATE_FORMAT))
      .withColumn("endDateF2", date_format(col("endDateF2"), FLOW_DATE_FORMAT))
      .filter(col("residuo_post") > 0.0)

    (residuoPre, residuoPost)
  }


  def adjustF3Calculation(dailyC: DataFrame): DataFrame = {
    dailyC
      .withColumn(DailyConsumptionAGGSBGSchema.value, when(
        col(DailyConsumptionAGGSBGSchema.idFormula).isin(2,3).or(col("startDateF2").isNotNull),
        col(DailyConsumptionAGGSBGSchema.value) * col(DailyConsumptionAGGSBGSchema.pprof))
        .otherwise(col(DailyConsumptionAGGSBGSchema.value)))
      .withColumn(DailyConsumptionAGGSBGSchema.valuef3, col(DailyConsumptionAGGSBGSchema.valuef3) * col(DailyConsumptionAGGSBGSchema.pprof))
  }

  def adjustF2Calculation(dailyC: DataFrame): DataFrame = {
    //val tiny = Double.MinPositiveValue

    val dailyWithInfos = dailyC
      .withColumn(
      "date_couples_hash",
      when(col("startDateF2").isNull.or(col("endDateF2").isNull), lit(null))
        .otherwise(md5(concat_ws("_", col("startDateF2"), col("endDateF2"))))
    )

    val window = Window
      .partitionBy(col(DailyConsumptionAGGSBGSchema.pdr), col("date_couples_hash"))

    val semiFinalTable =
      dailyWithInfos
        .withColumn("norm_coeff",
          sum(when(col("date_couples_hash").isNotNull.and(col(DailyConsumptionAGGSBGSchema.pprof) >= 0.0), col(DailyConsumptionAGGSBGSchema.pprof)).otherwise(lit(null))).over(window))
        .withColumn(
          "norm_coeff",
          when(col("norm_coeff").isNull && col("residuo").isNull, lit(null))
            .otherwise(coalesce(col("norm_coeff"), lit(0.0)) + coalesce(col("residuo"), lit(0.0)))
        )
        .withColumn("indet_flag", col("norm_coeff") === lit(0.0))
        .withColumn("norm_coeff", when(col("indet_flag"), lit(1.0)).otherwise(col("norm_coeff")))
        .withColumn(DailyConsumptionAGGSBGSchema.value, when(
          col("date_couples_hash").isNotNull,
          col(DailyConsumptionAGGSBGSchema.value) / col("norm_coeff"))
          .otherwise(col(DailyConsumptionAGGSBGSchema.value)))

    val finalTable =
      semiFinalTable
        .withColumn(DailyConsumptionAGGSBGSchema.errorCode, when(
          (col(DailyConsumptionAGGSBGSchema.value) < 0).and(col(DailyConsumptionAGGSBGSchema.errorCode).isin(11, 10, 1, 0))
          , lit(ErrorEnum.CONSUMPTION_IS_NEGATIVE_ERROR_CODE.id))
          .otherwise(col(DailyConsumptionAGGSBGSchema.errorCode)))
        .withColumn(DailyConsumptionAGGSBGSchema.value, when(col(DailyConsumptionAGGSBGSchema.value) < 0, lit(0.0).cast(DoubleType)).otherwise(col(DailyConsumptionAGGSBGSchema.value)))
        .withColumn(DailyConsumptionAGGSBGSchema.valuef3, when(col(DailyConsumptionAGGSBGSchema.valuef3) < 0, lit(0.0).cast(DoubleType)).otherwise(col(DailyConsumptionAGGSBGSchema.valuef3)))
        .withColumn(DailyConsumptionAGGSBGSchema.value, when(col("indet_flag"), lit(0.0)).otherwise(col(DailyConsumptionAGGSBGSchema.value)))
        .withColumn(DailyConsumptionAGGSBGSchema.errorCode, when(
          col("indet_flag").and(col(DailyConsumptionAGGSBGSchema.errorCode).isin(0))
          , lit(ErrorEnum.CONSUMPTION_UNDEFINED_ERROR_CODE.id))
          .otherwise(col(DailyConsumptionAGGSBGSchema.errorCode)))
        .selectExpr(DailyConsumptionAGGSBGSchema.getValues:_*)

    finalTable
  }
}
