package it.eng.au.pubblicazionePcg.controller

import it.eng.au.pubblicazionePcg.dao.sbg.SbgMisureDAO
import it.eng.au.pubblicazionePcg.schema.{DailyConsumptionSchema, OutputCsvSchema, SbgMisureSchema}
import it.eng.au.pubblicazionePcg.utility.Constants.PATTERNS
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, UserDefinedFunction}

import java.util.Properties
import scala.collection.immutable.ListMap

object PubblicazionePCG extends RunnableAggregator {
  override val key = "piva_rdb"
  override val baseName = "CLG2"

  def importMisure(df: DataFrame)(implicit prop: Properties, sqlContext: SQLContext): DataFrame = {
    val sbgMisureDAO = new SbgMisureDAO()

    val sbgType = prop.getProperty("sbg.type")
    val timestamp = prop.getProperty("date.run")
    val annoMese = prop.getProperty("year.month")
    val outputPath = sbgMisureDAO.hdfsOutput + "/" + sbgMisureDAO.partitionColumn + "=" + annoMese

    val calcoloConsumiSbg = df
      .select(col(DailyConsumptionSchema.pdr).alias(SbgMisureSchema.cod_pdr),
        col(DailyConsumptionSchema.pivaudd).alias(SbgMisureSchema.piva_udd),
        col(DailyConsumptionSchema.pivait).alias(SbgMisureSchema.piva_it),
        col(DailyConsumptionSchema.tipocliente).alias(SbgMisureSchema.tipo_cliente),
        col(DailyConsumptionSchema.unitmisprel).alias(SbgMisureSchema.unit_mis_prel),
        col(DailyConsumptionSchema.pivaudb).alias(SbgMisureSchema.piva_udb),
        col(DailyConsumptionSchema.codremi).alias(SbgMisureSchema.cod_remi),
        col(DailyConsumptionSchema.treatment).alias(SbgMisureSchema.trattamento),
        dayofmonth(to_date(col(DailyConsumptionSchema.date))).alias(SbgMisureSchema.giorno).cast(StringType),
        col(DailyConsumptionSchema.codprofstd).alias(SbgMisureSchema.cod_prof_std),
        col(DailyConsumptionSchema.idregclim).alias(SbgMisureSchema.id_reg_clim).cast(StringType),
        col(DailyConsumptionSchema.value).alias(SbgMisureSchema.consumo),
        col(DailyConsumptionSchema.pivardb).alias(SbgMisureSchema.piva_rdb))
      .withColumn(SbgMisureSchema.trattamento_calcolo, lit(null).cast(StringType))
      .withColumn(SbgMisureSchema.sessione_sbg, lit(sbgType))
      .withColumn(SbgMisureSchema.data_insert, lit(timestamp))
      .withColumn(SbgMisureSchema.annomese_rif, lit(annoMese))
      .selectExpr(SbgMisureSchema.getValues: _*)
      .repartition(sbgMisureDAO.repartitionFactor)

    sbgMisureDAO.writeParquetDirectlyToPath(calcoloConsumiSbg, outputPath)
    calcoloConsumiSbg
  }

  override def prepareDataFrame(df: DataFrame)(implicit prop: Properties, sqlContext: SQLContext): RDD[(ListMap[String, String], Row)] = {
    val filteredDf =
      df
        .drop(col(SbgMisureSchema.piva_udd))
        .drop(col(SbgMisureSchema.piva_udb))
        .drop(col(SbgMisureSchema.trattamento_calcolo))
        .drop(col(SbgMisureSchema.data_insert))
        .drop(col(SbgMisureSchema.sessione_sbg))
        .where(col(SbgMisureSchema.trattamento) === "G")

    val pivotDF = applyPivot(filteredDf)
    val completeDF = addMissingColumns(pivotDF, OutputCsvSchema.dailyColumns)
    val finalDF = replaceNullsWithEmptyString(completeDF)

    getPairedRdd(finalDF)
  }

  def applyPivot(df: DataFrame)(implicit prop: Properties): DataFrame = {
    df
      .withColumn(SbgMisureSchema.cod_pdr, patterns_replace(PATTERNS)(col(SbgMisureSchema.cod_pdr)))
      .withColumn(SbgMisureSchema.consumo, round(col(SbgMisureSchema.consumo), 2).cast(StringType))
      .withColumn(SbgMisureSchema.id_reg_clim, col(SbgMisureSchema.id_reg_clim).cast(StringType))
      .withColumn("giorno", concat(lit("PRELIEVO_GIORN_"), col("giorno").cast(IntegerType)))
      .groupBy(col(SbgMisureSchema.cod_pdr),
        col(SbgMisureSchema.annomese_rif),
        col(SbgMisureSchema.piva_it),
        col(SbgMisureSchema.piva_rdb),
        col(SbgMisureSchema.cod_remi),
        col(SbgMisureSchema.id_reg_clim),
        col(SbgMisureSchema.cod_prof_std),
        col(SbgMisureSchema.trattamento),
        col(SbgMisureSchema.tipo_cliente),
        col(SbgMisureSchema.unit_mis_prel))
      .pivot("giorno")
      .agg(first(SbgMisureSchema.consumo))
      .withColumnRenamed(SbgMisureSchema.cod_pdr, OutputCsvSchema.COD_PDR)
      .withColumnRenamed(SbgMisureSchema.piva_it, OutputCsvSchema.PIVA_IT)
      .withColumnRenamed(SbgMisureSchema.cod_remi, OutputCsvSchema.COD_REMI)
      .withColumnRenamed(SbgMisureSchema.id_reg_clim, OutputCsvSchema.ID_REG_CLIM)
      .withColumnRenamed(SbgMisureSchema.cod_prof_std, OutputCsvSchema.COD_PROF_PREL_STD)
      .withColumnRenamed(SbgMisureSchema.trattamento, OutputCsvSchema.TRATTAMENTO)
      .withColumnRenamed(SbgMisureSchema.tipo_cliente, OutputCsvSchema.TIPO_CLIENTE)
      .withColumnRenamed(SbgMisureSchema.unit_mis_prel, OutputCsvSchema.UN_MIS_PREL)
      .withColumnRenamed(SbgMisureSchema.annomese_rif, OutputCsvSchema.ANNOMESE)
  }

  def patterns_replace(patterns: Map[String, String]): UserDefinedFunction = udf((text: String) =>
    patterns.foldLeft(text) { case (text, (replacement, pattern)) =>
      text.replaceAll(pattern, replacement)
    }
  )

  def addMissingColumns(df: DataFrame, expectedColumnsInput: List[String]): DataFrame = {
    expectedColumnsInput.foldLeft(df) {
      (df, column) => {
        if (!df.columns.contains(column)) {
          df.withColumn(column, lit(null).cast(StringType))
        }
        else df
      }
    }
  }

  def replaceNullsWithEmptyString(df: DataFrame): DataFrame = {
    df.na.fill("")
  }

  def getPairedRdd(df: DataFrame)(implicit prop: Properties): RDD[(ListMap[String, String], Row)] = {
    distribution(df)
      .map(row => {
        val listMap = ListMap(
          key -> row.getAs[String](key),
          counterCsv -> row.getAs[String](counterCsv))

        (listMap, row)
      })
  }

  def distribution(df: DataFrame)(implicit prop: Properties): DataFrame = {
    val numLinesPerCsv = getNumLinesPerCsv
    val numCsvFiles = (df.count().toFloat / numLinesPerCsv).ceil.toInt

    df
      .withColumn(counterCsv, (monotonically_increasing_id % numCsvFiles).cast(StringType))
      .repartition(2000, col(key), col(counterCsv))
  }
}