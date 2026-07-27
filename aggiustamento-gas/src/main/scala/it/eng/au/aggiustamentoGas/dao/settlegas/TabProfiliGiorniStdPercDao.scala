package it.eng.au.aggiustamentoGas.dao.settlegas

import it.eng.au.aggiustamentoGas.dao.Dao
import it.eng.au.aggiustamentoGas.dao.measure.MeasureDAO.parseDateToOption
import it.eng.au.aggiustamentoGas.model.settlegas.TabProfiliGiorniStdPerc
import it.eng.au.aggiustamentoGas.schema.agg.DailyConsumptionAGGSBGSchema
import it.eng.au.aggiustamentoGas.schema.settlegas.TabProfiliGiorniStdPercSchema
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.{FLOW_DATE_FORMAT, TIMESTAMP_FORMAT}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.log.LogUtility.log
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import org.joda.time.format.DateTimeFormat

/** Contiene le informazioni del pprofk per un dato giorno, codice profilo e id regione climatica.
 * Il pprofk è un coefficiente utilizzato nella formula 3 del calcolo dei consumi. */
class TabProfiliGiorniStdPercDao extends Dao {
  override val parquetPath: String = Environment.getTabProfiliStdPercPath
  val tableName: String = Environment.getTabProfiliStdPercTableName

  override val columns: List[String] = List(
    TabProfiliGiorniStdPercSchema.data,
    TabProfiliGiorniStdPercSchema.prof,
    TabProfiliGiorniStdPercSchema.id_reg_clim,
    TabProfiliGiorniStdPercSchema.pprofk,
    TabProfiliGiorniStdPercSchema.cod_remi
  )

  val mapFunc: Row => TabProfiliGiorniStdPerc = (r: Row) => {
    TabProfiliGiorniStdPerc(
      data = parseDateToOption(r.getAs[String](TabProfiliGiorniStdPercSchema.data)).get.toString("yyyyMMdd"),
      prof = r.getAs[String](TabProfiliGiorniStdPercSchema.prof).trim,
      idRegClim = r.getAs[Int](TabProfiliGiorniStdPercSchema.id_reg_clim),
      pprofkPercentage = r.getAs[Double](TabProfiliGiorniStdPercSchema.pprofk) / 100
    )
  }

  def get(startDateCompute: String, endDateCompute: String, startDateCalculation: String, endDateCalculation: String): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val startDateComputed = DateTimeFormat.forPattern("yyyyMM").parseDateTime(startDateCompute).dayOfMonth().withMinimumValue().withTimeAtStartOfDay().toString(TIMESTAMP_FORMAT)
    val endDateComputed = DateTimeFormat.forPattern("yyyyMM").parseDateTime(endDateCompute).dayOfMonth().withMaximumValue().withTimeAtStartOfDay().toString(TIMESTAMP_FORMAT)
    val startDateCalculated = DateTimeFormat.forPattern("yyyyMM").parseDateTime(startDateCalculation).dayOfMonth().withMinimumValue().withTimeAtStartOfDay().toString(TIMESTAMP_FORMAT)
    val endDateCalculated = DateTimeFormat.forPattern("yyyyMM").parseDateTime(endDateCalculation).dayOfMonth().withMaximumValue().withTimeAtStartOfDay().toString(TIMESTAMP_FORMAT)

    val tabProfFull = Environment.getSpark.sqlContext.read.table(tableName).selectExpr(columns: _*)
      .withColumn(TabProfiliGiorniStdPercSchema.data, to_timestamp(col(TabProfiliGiorniStdPercSchema.data), FLOW_DATE_FORMAT))
      .withColumn(TabProfiliGiorniStdPercSchema.pprofk, col(TabProfiliGiorniStdPercSchema.pprofk) / 100)
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.prof, DailyConsumptionAGGSBGSchema.codProfStd)
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.id_reg_clim, DailyConsumptionAGGSBGSchema.idRegClim)
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.cod_remi, DailyConsumptionAGGSBGSchema.codRemi)
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.data, DailyConsumptionAGGSBGSchema.date)

    val tabProfInMonths =
      tabProfFull
        .filter(col(DailyConsumptionAGGSBGSchema.date).between(lit(startDateCalculated).cast(TimestampType), lit(endDateCalculated).cast(TimestampType)))

    val tabProfBeforeRemiInMonths = tabProfInMonths
      .filter(col(DailyConsumptionAGGSBGSchema.date) <= to_timestamp(lit("2025-09-30 23:59:59")))
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.pprofk, TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_1")

    val tabProfAfterRemiInMonths = tabProfInMonths
      .filter(col(DailyConsumptionAGGSBGSchema.date) > to_timestamp(lit("2025-09-30 23:59:59")))
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.pprofk, TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_2")

    val tabProfOutMonths =
      tabProfFull
        .filter(col(DailyConsumptionAGGSBGSchema.date).between(lit(startDateComputed).cast(TimestampType), lit(endDateComputed).cast(TimestampType)))
        .filter(!col(DailyConsumptionAGGSBGSchema.date).between(lit(startDateCalculated).cast(TimestampType), lit(endDateCalculated).cast(TimestampType)))

    val tabProfBeforeRemiOutMonths = tabProfOutMonths
      .filter(col(DailyConsumptionAGGSBGSchema.date) <= to_timestamp(lit("2025-09-30 23:59:59")))
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.pprofk, TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_3")
      .withColumnRenamed(DailyConsumptionAGGSBGSchema.date, DailyConsumptionAGGSBGSchema.date.toString ++ "_3")

    val tabProfAfterRemiOutMonths = tabProfOutMonths
      .filter(col(DailyConsumptionAGGSBGSchema.date) > to_timestamp(lit("2025-09-30 23:59:59")))
      .withColumnRenamed(TabProfiliGiorniStdPercSchema.pprofk, TabProfiliGiorniStdPercSchema.pprofk.toString ++ "_4")
      .withColumnRenamed(DailyConsumptionAGGSBGSchema.date, DailyConsumptionAGGSBGSchema.date.toString ++ "_4")

    (tabProfBeforeRemiInMonths, tabProfAfterRemiInMonths, tabProfBeforeRemiOutMonths, tabProfAfterRemiOutMonths)

  }
}
