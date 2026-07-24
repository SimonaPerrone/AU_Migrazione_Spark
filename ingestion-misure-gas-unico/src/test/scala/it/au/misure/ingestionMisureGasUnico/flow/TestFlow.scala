package it.au.misure.ingestionMisureGasUnico.flow

//import it.au.misure.ingestionMisureGasUnico.utility.SparkLocal
import junit.framework.TestCase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.math.BigInteger
import it.au.misure.ingestionMisureGasUnico.flow.standard.m.{A01StandardFlow, TGLStandardFlow, TMLStandardFlow}
import it.au.misure.ingestionMisureGasUnico.flow.standard.r.{RGLStandardFlow, RSLStandardFlow}
import it.au.misure.ingestionMisureGasUnico.utility.EnvironmentSparkTest
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import org.junit.Assert

class TestFlow extends /*TestCase with SparkLocal*/ EnvironmentSparkTest {
  def testAddCommonColumns(): Unit = {
    val filePathName = "/tmp"
    val fileName = "name.xml"
    val unzipTimestamp = "2020-11-10T16:46:56.243000"

    val nonNullDf = Environment.getSpark.sqlContext.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(List(Row(
        fileName
        , s"$filePathName/$fileName"
        , "2020"
        , "2019"
        , "01"
        , "12"
        , "12/2018"
        , "17/01/1964"
        , "17/01/1965"
        , "17/01/1966"
        , "17/01/1967"
      )))
      , StructType(List(
        StructField("t_name_file", StringType, nullable = false)
        , StructField("local_file", StringType, nullable = false)
        , StructField("anno", StringType, nullable = false)
        , StructField("anno_riferimento", StringType, nullable = false)
        , StructField("mese", StringType, nullable = false)
        , StructField("mese_riferimento", StringType, nullable = false)
        , StructField("mese_comp", StringType, nullable = true)
        , StructField("data_racc", StringType, nullable = true)
        , StructField("data_comp", StringType, nullable = true)
        , StructField("data_prest", StringType, nullable = true)
        , StructField("data_misura", StringType, nullable = true)
      ))
    )

    val nullDf = Environment.getSpark.sqlContext.createDataFrame(
      Environment.getSpark.sparkContext.parallelize(List(Row(
        fileName
        , s"$filePathName/$fileName"
        , "2020"
        , "2019"
        , "01"
        , "12"
        , null
        , null
        , null
        , null
        , null
      )))
      , StructType(List(
        StructField("t_name_file", StringType, nullable = false)
        , StructField("local_file", StringType, nullable = false)
        , StructField("anno", StringType, nullable = false)
        , StructField("anno_riferimento", StringType, nullable = false)
        , StructField("mese", StringType, nullable = false)
        , StructField("mese_riferimento", StringType, nullable = false)
        , StructField("mese_comp", StringType, nullable = true)
        , StructField("data_racc", StringType, nullable = true)
        , StructField("data_comp", StringType, nullable = true)
        , StructField("data_prest", StringType, nullable = true)
        , StructField("data_misura", StringType, nullable = true)
      ))
    )

    val fileNameMD5MD = MessageDigest.getInstance("MD5")
    fileNameMD5MD.update(StandardCharsets.UTF_8.encode(fileName))
    val fileNameMD5 = "%032x".format(new BigInteger(1, fileNameMD5MD.digest))

    val filePathMD5MD = MessageDigest.getInstance("MD5")
    filePathMD5MD.update(StandardCharsets.UTF_8.encode(s"$filePathName/$fileName"))
    val filePathMD5 = "%032x".format(new BigInteger(1, filePathMD5MD.digest))

    val tglRow = TGLStandardFlow.addCommonColumns(nonNullDf, unzipTimestamp).head
    val tmlRow = TMLStandardFlow.addCommonColumns(nonNullDf, unzipTimestamp).head
    val a01Row = A01StandardFlow.addCommonColumns(nonNullDf, unzipTimestamp).head
    val rglRow = RGLStandardFlow.addCommonColumns(nonNullDf, unzipTimestamp).head
    val rslRow = RSLStandardFlow.addCommonColumns(nonNullDf, unzipTimestamp).head
    val igmgRow = IGMGFlow.addCommonColumns(nonNullDf, unzipTimestamp).head
    val tglNullRow = TGLStandardFlow.addCommonColumns(nullDf, unzipTimestamp).head
    val tmlNullRow = TMLStandardFlow.addCommonColumns(nullDf, unzipTimestamp).head
    val a01NullRow = A01StandardFlow.addCommonColumns(nullDf, unzipTimestamp).head
    val rglNullRow = RGLStandardFlow.addCommonColumns(nullDf, unzipTimestamp).head
    val rslNullRow = RSLStandardFlow.addCommonColumns(nullDf, unzipTimestamp).head
    val igmgNullRow = IGMGFlow.addCommonColumns(nullDf, unzipTimestamp).head

    Assert.assertEquals(fileNameMD5, tglRow.getAs[String]("n_id"))
    Assert.assertEquals(filePathMD5, tglRow.getAs[String]("n_id_file"))
    Assert.assertEquals("201912", tglRow.getAs[String]("annomese_riferimento"))
    Assert.assertEquals(unzipTimestamp, tglRow.getAs[String]("d_caricamento"))
    Assert.assertEquals(unzipTimestamp, tglRow.getAs[String]("dataelaborazione"))

    Assert.assertEquals("122018", tglRow.getAs[String]("mese_comp"))
    Assert.assertEquals("122018", rglRow.getAs[String]("mese_comp"))
    Assert.assertEquals("EE", tglNullRow.getAs[String]("mese_comp"))
    Assert.assertEquals("EE", rglNullRow.getAs[String]("mese_comp"))

    Assert.assertEquals("196501", tglRow.getAs[String]("annomese"))
    Assert.assertEquals("196401", tmlRow.getAs[String]("annomese"))
    Assert.assertEquals("196601", a01Row.getAs[String]("annomese"))
    Assert.assertEquals("196401", rglRow.getAs[String]("annomese"))
    Assert.assertEquals("196601", rslRow.getAs[String]("annomese"))
    Assert.assertEquals("196701", igmgRow.getAs[String]("annomese"))
    Assert.assertEquals("EE", tglNullRow.getAs[String]("annomese"))
    Assert.assertEquals("EE", tmlNullRow.getAs[String]("annomese"))
    Assert.assertEquals("EE", a01NullRow.getAs[String]("annomese"))
    Assert.assertEquals("EE", rglNullRow.getAs[String]("annomese"))
    Assert.assertEquals("EE", rslNullRow.getAs[String]("annomese"))
    Assert.assertEquals("EE", igmgNullRow.getAs[String]("annomese"))
  }

  def testFlowDataPath(): Unit = {
    Assert.assertTrue(TGLStandardFlow.flowDataPath.contains(s"${TGLStandardFlow.flowType}/${TGLStandardFlow.flowName}"))
    Assert.assertTrue(RGLStandardFlow.flowDataPath.contains(s"${RGLStandardFlow.flowType}/${RGLStandardFlow.flowName}"))
    Assert.assertTrue(IGMGFlow.flowDataPath.contains(s"${IGMGFlow.flowType}/${IGMGFlow.flowName}"))

    Assert.assertFalse(TGLStandardFlow.flowDataPath.contains("null"))
    Assert.assertFalse(RGLStandardFlow.flowDataPath.contains("null"))
    Assert.assertFalse(IGMGFlow.flowDataPath.contains("null"))
  }
}
