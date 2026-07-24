package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.model.{AlreadyComputedZips, ZipRzg1Metadata}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.junit.Assert

import java.io.File

class AlreadyComputedZipsControllerTest extends EnvironmentSparkTest {
  def testFilterAlreadyComputedZips(): Unit = {
    val zipRzg1Metadata = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File("path/file3.zip"), 123L, None, "udd3", "distr3", "udd3", "2022", "10", "202208", "20221121102030", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("path/file1.zip"), 123L, None, "udd1", "distr1", "udd1", "2022", "11", "202208", "20221121102030", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("path/file2.zip"), 123L, None, "udd2", "distr2", "udd2", "2022", "11", "202208", "20221121102030", "1", isAmmissibile = true)
    ))

    val alreadyComputedZips = Environment.sparkContext.parallelize(Seq(
      AlreadyComputedZips("path\\file1.zip", 123L),
      AlreadyComputedZips("path\\file2.zip", 124L),
      AlreadyComputedZips("path\\file4.zip", 125L)
    ))

    val result = AlreadyComputedZipsController.filterAlreadyComputedZips(zipRzg1Metadata, alreadyComputedZips)
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(0, result.filter(zip => zip.file.getName == "file1.zip").count)
  }
}
