package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.model.{AggregatoTotale, CsvRzg1Metadata, ZipRzg1Metadata}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.junit.Assert

import java.io.File

class JoinControllerTest extends EnvironmentSparkTest {
  def testJoinZrg1Indennizzi(): Unit = {
    val correctCsv = CsvRzg1Metadata("", "", None, Some("123"), None, None, None, None, None, None, None)
    val badCsv1 = CsvRzg1Metadata("", "", None, Some(""), None, None, None, None, None, None, None)
    val badCsv2 = CsvRzg1Metadata("", "", None, None, None, None, None, None, None, None, None)

    val zipMetadata = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File(""), 123456L, Some(correctCsv), "udd1", "distr1", "udd1", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123456L, Some(correctCsv), "udd2", "distr2", "udd2", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123456L, Some(badCsv1), "udd3", "distr3", "udd3", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123456L, Some(badCsv2), "udd4", "distr4", "udd4", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123456L, Some(correctCsv), "udd5", "distr5", "udd5", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true)
    ))

    val aggregatoTotale = Environment.sparkContext.parallelize(Seq(
      AggregatoTotale(1L, "distr1", "udd1", Some(0.0), Some(0.0), Some(0.0)),
      AggregatoTotale(12L, "distr1", "udd1", Some(0.0), Some(0.0), Some(0.0)),
      AggregatoTotale(123L, "distr1", "udd1", Some(0.0), Some(0.0), Some(0.0)),
      AggregatoTotale(123L, "distr3", "udd3", Some(0.0), Some(0.0), Some(0.0)),
      AggregatoTotale(123L, "distr4", "udd4", None, None, None),
      AggregatoTotale(123L, "distr5", "udd5", Some(0.0), Some(0.0), Some(0.0)),
      AggregatoTotale(123L, "distr6", "udd6", Some(0.0), Some(0.0), Some(0.0)),
      AggregatoTotale(123L, "distr7", "udd7", Some(0.0), Some(0.0), Some(0.0))
    ))

    val result = JoinController.joinRzg1Indennizzi(zipMetadata, aggregatoTotale)
    result.collect.foreach(println)

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(2, result.filter(_._2.isDefined).count)
  }
}
