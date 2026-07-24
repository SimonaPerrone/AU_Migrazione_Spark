package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.model.ZipRzg1Metadata
import it.eng.au.ammissibilitaRendiconti.utility.constants.Constants.COD_919
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.junit.Assert

import java.io.File

class AlreadyTransmittedFileRzg1ControllerTest extends EnvironmentSparkTest {
  def testGetAlreadyTransmitted(): Unit = {
    val alreadyTransmittedZip = Environment.sparkContext.parallelize(Seq(
      "zip1", "zip3", "zip5"
    ))

    val zipRzg1Metadata = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File("test/zip1"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("test/zip2"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("test/zip3"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("test/zip4"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("test/zip5"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)
    ))

    val result = AlreadyTransmittedRzg1FileController.getAlreadyTransmitted(zipRzg1Metadata, alreadyTransmittedZip)
    result.collect.foreach(println)

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(3, result.filter(_.statusCode.equals(COD_919)).count)
    Assert.assertEquals(2, result.filter(_.isAmmissibile.equals(true)).count)
  }
}
