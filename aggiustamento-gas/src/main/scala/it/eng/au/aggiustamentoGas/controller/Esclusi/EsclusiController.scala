package it.eng.au.aggiustamentoGas.controller.Esclusi

import it.eng.au.aggiustamentoGas.schema.agg.{DailyConsumptionAGGSBGSchema, DailyConsumptionEsclusiSchema}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, count, lit, max, min, not, pmod, substring, when}
import org.apache.spark.sql.types.IntegerType

class EsclusiController {

  val isPdrAnomalousGDM = "is_pdr_anomalous_gdm"
  val isDayAnomalous: String = "is_day_anomalous"
  val dayOfMonth = "dayOfMonth"
  val pdrCountColName = "pdr_count"
  val isForcedExcluded = "is_forced_excluded"
  val isF3Valued = "is_f3_valued"


  def pdrCountComparisonExpression: Column = when(substring(col(DailyConsumptionAGGSBGSchema.annoMese), 5, 2).isin("04", "06", "09", "11"), 30).when(substring(col(DailyConsumptionAGGSBGSchema.annoMese), 5, 2).isin("01", "03", "05", "07", "08", "10", "12"), 31).when(substring(col(DailyConsumptionAGGSBGSchema.annoMese), 5, 2).isin("02") and pmod(substring(col(DailyConsumptionAGGSBGSchema.annoMese), 1, 4).cast(IntegerType), lit(4)) === 0, 29).otherwise(28)

  def windowForComparison: WindowSpec = Window.partitionBy(
    DailyConsumptionAGGSBGSchema.pdr,
    DailyConsumptionAGGSBGSchema.pivaDistr,
    DailyConsumptionAGGSBGSchema.pivaIt,
    DailyConsumptionAGGSBGSchema.pivaUdd,
    DailyConsumptionAGGSBGSchema.pivaUdb,
    DailyConsumptionAGGSBGSchema.pivaRdb,
    DailyConsumptionAGGSBGSchema.treatment,
    DailyConsumptionAGGSBGSchema.session,
    DailyConsumptionAGGSBGSchema.annoMese
  )


  def getExcludedPdrs(df: DataFrame): DataFrame = {
    val valueNotSterilized = "valueNotSterilized"
    df
      .na.fill("Y", Seq(DailyConsumptionAGGSBGSchema.treatment.toString))
      .filter((col(DailyConsumptionAGGSBGSchema.forceExclusion) <=> true) or (
        (col(DailyConsumptionAGGSBGSchema.isValid) === true or (col(DailyConsumptionAGGSBGSchema.isValid) === false and !(col(DailyConsumptionAGGSBGSchema.idFormula) === 3))) and
          not(col(DailyConsumptionAGGSBGSchema.errorCode).isin(0, 7, 10, 11, 12, 13, 14, 15)) and
          col(DailyConsumptionAGGSBGSchema.codRemi).isNotNull and
          col(DailyConsumptionAGGSBGSchema.dtg).isNotNull and
          col(DailyConsumptionAGGSBGSchema.pivaUdb).isNotNull and
          col(DailyConsumptionAGGSBGSchema.pivaUdd).isNotNull and
          col(DailyConsumptionAGGSBGSchema.pivaDistr).isNotNull and
          col(DailyConsumptionAGGSBGSchema.pivaIt).isNotNull and
          col(DailyConsumptionAGGSBGSchema.pivaRdb).isNotNull and
          col(DailyConsumptionAGGSBGSchema.tipoCliente).isNotNull and
          col(DailyConsumptionAGGSBGSchema.unitMisPrel).isNotNull
        )
      )
      .withColumn(pdrCountColName, count(col(DailyConsumptionAGGSBGSchema.pdr)).over(windowForComparison))
      .withColumn(isForcedExcluded, min(col(DailyConsumptionAGGSBGSchema.forceExclusion)).over(windowForComparison))
      .where(col(isForcedExcluded) or (col(pdrCountColName) === pdrCountComparisonExpression))
      .withColumn(valueNotSterilized, lit(col(DailyConsumptionAGGSBGSchema.value)))
      .withColumn(DailyConsumptionAGGSBGSchema.value, lit(col(DailyConsumptionAGGSBGSchema.valuef3)))
      .coalesce(df.rdd.getNumPartitions)
      .repartition(df.rdd.getNumPartitions)
      .selectExpr(DailyConsumptionEsclusiSchema.getValues: _*)
  }

}
