package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.junit.Assert

import java.io.File

class ReadZipControllerTest extends EnvironmentSparkTest {
  val path = "src/test/resources/input/read_zip_controller"

  def testReadZip(): Unit = {
    Environment.setProperty("recovery.mode", "false")
    Environment.setProperty("rzg1Zip.input.path", path)
    Environment.setProperty("year.month", "202210")
    Environment.setProperty("current.year", "2022")
    Environment.setProperty("current.month", "11")

    val (result, _, _) = ReadZipController.readZip()
    result.collect.foreach(println)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(2, result.filter(_.isAmmissibile).count)
  }

  def testReadZipRecoveryMode(): Unit = {
    Environment.setProperty("recovery.mode", "true")
    Environment.setProperty("recovery.csv.path", "src/test/resources/recovery_file.csv")

    val (result, _, _) = ReadZipController.readZip()
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_.isAmmissibile).count)
  }

  def testFlatAllZip(): Unit = {
    val currentYear = "2022"
    val currentMonth = "09"

    val rdd = ReadZipController.flatAllZip(path, currentYear, currentMonth)
    rdd.collect.foreach(println)

    Assert.assertEquals(2, rdd.count)
  }

  def testZipFileWithMetadata(): Unit = {
    val fileRdd = Environment.sparkContext.parallelize(Seq(
      new File(path + "/CIG1_udd1/2022/10/distr1_udd1_202210_RZG1_20221027161000_1.zip"),
      new File(path + "/CIG1_udd1/2022/10/distr1_udd2_202210_RZG1_20221027161100_1.zip"),
      new File(path + "/CIG1_udd1/2022/10/distr2_udd2_202210_RZG1_20221027161200_1.zip"),
      new File(path + "/CIG1_udd1/2022/10/distr2_udd2_202210_RZG1_20221027161200.zip"),
      new File(path + "/CIG1_udd1/2022/10/distr2_udd2_202210_RZG1_20221027161200.csv"))
    )

    val result = ReadZipController.getZipMetadata(fileRdd)
    result.collect.foreach(println)

    Assert.assertEquals(4, result.count)
    Assert.assertEquals(3, result.filter(_.isAmmissibile).count)
  }
}
