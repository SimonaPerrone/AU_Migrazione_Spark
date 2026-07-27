package it.sferanet.au.utility

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.functions.{col, lit, when}


case class CreateCustomParquet() extends EnvironmentSparkTest {

  def testCreateFakeParquetSw1(): Unit = {
    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/sw1/old_route/template")
      .filter(col("cod_pdr") === lit("15270000000546"))
      .withColumn("segn_mis_eff", lit("000028375"))
      .withColumn("segn_conv_eff", lit("000028376"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/sw1/compilated/version_eff")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/sw1/old_route/template")
      .filter(col("cod_pdr") === lit("15270000000820"))
      .drop("segn_mis_sost")
      .drop("segn_conv")
      .withColumn("segn_mis_eff", lit("000028371"))
      .withColumn("segn_conv_eff", lit("000028373"))
      .withColumnRenamed("segn_mis_eff", "segn_mis_sost")
      .withColumnRenamed("segn_conv_eff", "segn_conv")
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/sw1/compilated/version_sost")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/sw1/old_route/template")
      .withColumn("ammissibilita", lit("OK"))
      .withColumn("cod_pdr", when(col("cod_pdr") === "15270000000546", lit("fake1")).otherwise(lit("fake2")))
      .filter(col("cod_pdr") === lit("fake1"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/sw1/compilated/version_standard")
  }

  def testCreateFakeParquetSwg1(): Unit = {
    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/swg1/template")
      .drop("data_prest")
      .drop("let_tot_prel")
      .drop("let_tot_conv")
      .withColumn("segn_conv_eff", lit("000052131"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/swg1/compilated/version_eff")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/swg1/template")
      .withColumn("cod_pdr", lit("08200000016471"))
      .drop("data_mis_eff")
      .drop("segn_mis_eff")
      .drop("segn_conv_eff")
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/swg1/compilated/version_sost")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/swg1/template")
      .withColumn("cod_pdr", lit("fake1"))
      .drop("ammissibilita")
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/swg1/compilated/version_old")
  }

  def testCreateFakeParquetTmvOld(): Unit = {

    val measureOldSost = "segn_mis_sost"
    val convertedOldSost = "segn_conv"
    val measure = "segn_mis_eff"
    val converted = "segn_conv_eff"
    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/template_old")
      .drop(measureOldSost)
      .drop(convertedOldSost)
      .withColumn(measure, lit("000052132"))
      .withColumn(converted, lit("000052133"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/compilated_old/version_eff")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/template_old")
      .withColumn("cod_pdr", lit("08200000016471"))
      .drop(measure)
      .drop(converted)
      .withColumn("cod_pdr", lit("08200000016471"))
      .withColumn(measureOldSost, lit("000052134"))
      .withColumn(convertedOldSost, lit("000052135"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/compilated_old/version_sost")


  }

  def testCreateFakeParquetTmvStandard(): Unit = {

    /*standard*/
    val dataStandardSost = "data_prest"
    val measureStandardSost = "let_tot_prel"
    val convertedStandardSost = "let_tot_conv"

    val date = "data_mis_eff"
    val measure = "segn_mis_eff"
    val converted = "segn_conv_eff"
    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/template_new")
      .drop(dataStandardSost)
      .drop(measureStandardSost)
      .drop(convertedStandardSost)
      .withColumn(measure, lit("000052137"))
      .withColumn(converted, lit("000052138"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/compilated_new/version_eff")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/template_new")
      .drop(date)
      .drop(measure)
      .drop(converted)
      .withColumn("cod_pdr", lit("03390000023950"))
      .withColumn(dataStandardSost, lit("12/12/2020"))
      .withColumn(measureStandardSost, lit("000052140"))
      .withColumn(convertedStandardSost, lit("000052141"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/tmv/compilated_new/version_sost")


  }

  def testCreateFakeParquetFui(): Unit = {

    val data = "data_mis_eff"
    val measure = "segn_mis_eff"
    val converted = "segn_conv_eff"

    val dataSost = "data_prest"
    val measureSost = "let_tot_prel"
    val convertedSost = "let_tot_conv"

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/fui/template")
      .drop(dataSost)
      .drop(measureSost)
      .drop(convertedSost)
      .withColumn(measureSost, lit("000000100"))
      .withColumn(convertedSost, lit("000000105"))
      .filter(col("cod_pdr") === lit("05260200281588"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/fui/compilated/version_eff")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/fui/template")
      .drop(data)
      .drop(measure)
      .drop(converted)
      .filter(col("cod_pdr") === lit("15810000024807"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/fui/compilated/version_sost")

  }

  def testCreateFakeParquetFdd(): Unit = {

    val data = "data_mis_eff"
    val measure = "segn_mis_eff"
    val converted = "segn_conv_eff"

    val dataSost = "data_prest"
    val measureSost = "let_tot_prel"
    val convertedSost = "let_tot_conv"

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/fdd/template")
      .drop(dataSost)
      .drop(measureSost)
      .drop(convertedSost)
      .withColumn(measure, lit("000000100"))
      .withColumn(converted, lit("000000105"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/fdd/compilated/version_eff")

    Environment.getSqlContext
      .read
      .parquet("./src/test/resources/integrazione-ca/misure/fdd/template")
      .drop(data)
      .drop(measure)
      .drop(converted)
      .withColumn(measureSost, lit("000000101"))
      .withColumn(convertedSost, lit("000000106"))
      .withColumn("cod_pdr", lit("15810000024808"))
      .coalesce(1).write
      .mode("overwrite")
      .parquet("./src/test/resources/integrazione-ca/misure/fdd/compilated/version_sost")

  }
}
