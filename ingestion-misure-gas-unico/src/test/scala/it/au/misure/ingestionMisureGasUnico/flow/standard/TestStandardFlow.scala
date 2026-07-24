package it.au.misure.ingestionMisureGasUnico.flow.standard

import it.au.misure.ingestionMisureGasUnico.flow.standard.m.TGLStandardFlow
import it.au.misure.ingestionMisureGasUnico.utility.EnvironmentSparkTest
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
//import it.au.misure.ingestionMisureGasUnico.utility.SparkLocal
import junit.framework.TestCase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.Assert

class TestStandardFlow extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {
  def testRenameColumns(): Unit = {
    val tglDf = Environment.getSpark.sqlContext.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(List(Row("TGL")))
      , StructType(List(StructField("cod_flusso", StringType, nullable = false)))
    )

    val outputDf = TGLStandardFlow.renameColumns(tglDf)
    Assert.assertFalse(outputDf.columns.contains("cod_flusso"))
    Assert.assertTrue(outputDf.columns.contains("cod_servizio"))
  }

  def testAddNullColumns(): Unit = {
    val tglDf = Environment.getSpark.sqlContext.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(List(Row("TGL")))
      , StructType(List(StructField("cod_flusso", StringType, nullable = false)))
    )

    val outputDf = TGLStandardFlow.addNullColumns(tglDf)
    Assert.assertEquals(TGLStandardFlow.schema.getValues.length, outputDf.columns.length)
    val outputRow = outputDf.head()
    outputDf.columns
      .filterNot(_ == "cod_flusso")
      .foreach(columnName => Assert.assertNull(outputRow.getAs[String](columnName))
      )
  }
}
