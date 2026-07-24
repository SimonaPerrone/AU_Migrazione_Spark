package it.au.misure.ingestionMisureGasUnico.flow.standard.m

import it.au.misure.ingestionMisureGasUnico.flow.standard.StandardFlow
import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.CommonColumnsSchema._
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.TracciatoStandardEnum.{M, TracciatoStandardEnum}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.TGLSchema.{data_comp, data_prest, data_racc}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m._
import it.au.misure.ingestionMisureGasUnico.utility.Constants._
import it.au.misure.ingestionMisureGasUnico.utility.ParseUtility
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

trait MisuraFlow extends StandardFlow {
  override val tS: TracciatoStandardEnum = M

  override def parse(inputRdd: RDD[GasXmlMetadata])
                    /*(implicit sc: SparkContext, sqlContext: SQLContext)*/: DataFrame = {
    val dfRdd = inputRdd.flatMap(ParseUtility.parseXmlMisura)
    Environment.getSpark.sqlContext.createDataFrame(dfRdd, MisuraSchema.createSparkSchema())
  }

  override def addCommonColumns(df: DataFrame, unzipTimestamp: String): DataFrame = {
    val dateColumn: String = schema match {
      case TGLSchema => data_comp
      case TMLSchema | TASSchema | TALSchema | TAVSchema => data_racc
      case _ => data_prest
    }

    val commonDf = super.addCommonColumns(df, unzipTimestamp)
      .withColumn(annomese,
        from_unixtime(unix_timestamp(df(dateColumn), ITALIAN_DATE_PATTERN), ANNOMESE_PATTERN))

    commonDf
      .na.fill(EE.toString, List(annomese.toString))
  }
}
