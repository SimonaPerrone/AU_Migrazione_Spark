package it.sferanet.au.dal

import it.sferanet.au.model.{Weights, WeightsFields}
import it.sferanet.au.schema.TabProfStdPercSchema
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date}
import org.apache.spark.sql.types.DateType

import java.text.SimpleDateFormat


class WeightsTable(inputPath: String, fields: WeightsFields) extends Serializable {

  def this(inputPath: String) = {
    this(inputPath, WeightsTable.flowFields)
  }

  def get(): (RDD[Weights], DataFrame, DataFrame) = {
    val f = fields
    val df = Environment.getSqlContext.read.table(inputPath)
      .withColumnRenamed("DATA", TabProfStdPercSchema.data)
      .withColumnRenamed("PPROFK", TabProfStdPercSchema.pprofk)
      .withColumnRenamed("PPROFK_NORM", TabProfStdPercSchema.pprofk_norm)
      .withColumnRenamed("ID_REG_CLIM", TabProfStdPercSchema.id_reg_clim)
      .withColumnRenamed("PROF", TabProfStdPercSchema.prof)
      .withColumnRenamed("WKR", TabProfStdPercSchema.wkr)

    val weightsPreRemi = df.drop(TabProfStdPercSchema.cod_remi.toString)
      .withColumn("data_stdfmt", to_date(col(TabProfStdPercSchema.data), "dd/MM/yyyy"))
      .filter(col("data_stdfmt") < lit("2025-10-01").cast(DateType))
      .drop("data_stdfmt")
      .rdd
      .map(r => Weights(
        Constants.getDate(WeightsTable.format, r.getAs(f.date)).get, //data
        r.getAs[Double](f.pprofk),
        r.getAs[Double](f.pprofk_norm),
        if (r.getAs(f.id_reg_clim) == null) None else Some(r.getAs(f.id_reg_clim).toString.toDouble.toInt), //reg_clim
        r.getAs(f.prof), //prof
        r.getAs[Double](f.wkr) //cod_pdr
      )
      ).filter(_.id_reg_clim.isDefined)

    val preRemi = df.drop(TabProfStdPercSchema.cod_remi.toString)
      .withColumn(TabProfStdPercSchema.data, to_date(col(TabProfStdPercSchema.data), "dd/MM/yyyy"))
      .withColumn(TabProfStdPercSchema.pprofk, col(TabProfStdPercSchema.pprofk) / 100)
      .withColumn(TabProfStdPercSchema.pprofk_norm, col(TabProfStdPercSchema.pprofk_norm) / 100)
      .filter(col(TabProfStdPercSchema.id_reg_clim).isNotNull)
      .filter(col(TabProfStdPercSchema.data) < lit("2025-10-01").cast(DateType))

    val postRemi = df
      .withColumn(TabProfStdPercSchema.data, to_date(col(TabProfStdPercSchema.data), "dd/MM/yyyy"))
      .withColumn(TabProfStdPercSchema.pprofk, col(TabProfStdPercSchema.pprofk) / 100)
      .withColumn(TabProfStdPercSchema.pprofk_norm, col(TabProfStdPercSchema.pprofk_norm) / 100)
      .filter(col(TabProfStdPercSchema.id_reg_clim).isNotNull)
      .filter(col(TabProfStdPercSchema.data) >= lit("2025-10-01").cast(DateType))

    (weightsPreRemi, preRemi, postRemi)
  }
}

object WeightsTable {
  val flowFields: WeightsFields = WeightsFields(
    "data",
    "pprofk",
    "pprofk_norm",
    "id_reg_clim",
    "prof",
    "wkr"
  )

  def format: SimpleDateFormat = Constants.STANDARD_FORMAT_DATE
}
