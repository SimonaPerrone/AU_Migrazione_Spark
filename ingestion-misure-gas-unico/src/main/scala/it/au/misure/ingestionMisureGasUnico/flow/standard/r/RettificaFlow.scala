package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.flow.standard.StandardFlow
import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.CommonColumnsSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.TracciatoStandardEnum.{R, TracciatoStandardEnum}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RGLSchema.{data_prest, data_racc}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r._
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.ParseUtility
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

trait RettificaFlow extends StandardFlow {
  override val tS: TracciatoStandardEnum = R

  override def parse(inputRdd: RDD[GasXmlMetadata])
                    /*(implicit sc: SparkContext, sqlContext: SQLContext)*/: DataFrame = {
    val dfRdd = inputRdd.flatMap(ParseUtility.parseXmlRettifica)
    Environment.getSpark.sqlContext.createDataFrame(dfRdd, RettificaSchema.createSparkSchema())
  }

  override def addCommonColumns(df: DataFrame, unzipTimestamp: String): DataFrame = {
    val dateColumn: String = schema match {
      case RMLSchema | RGLSchema => data_racc
      case _ => data_prest
    }

    val commonDf = super.addCommonColumns(df, unzipTimestamp)
      .withColumn(annomese,
        from_unixtime(unix_timestamp(df(dateColumn), ITALIAN_DATE_PATTERN), ANNOMESE_PATTERN))

    commonDf
      .na.fill(EE.toString, List(annomese.toString))
  }
}
