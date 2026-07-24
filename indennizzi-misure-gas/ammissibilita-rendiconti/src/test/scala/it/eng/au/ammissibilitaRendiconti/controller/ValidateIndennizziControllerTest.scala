package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.controller.ValidateIndennizziController.{ruleCheckAtLeastOneOMIsValued, ruleValidateIdIndennizzo, ruleValidateOM1, ruleValidateOM2, ruleValidateOM3}
import it.eng.au.ammissibilitaRendiconti.model.rules.IndennizziRule
import it.eng.au.ammissibilitaRendiconti.model.{AggregatoTotale, CsvRzg1Metadata, ZipRzg1Metadata}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.junit.Assert

import java.io.File

class ValidateIndennizziControllerTest extends EnvironmentSparkTest {
  def testValidate(): Unit = {
    val header = "DATA;ID_INDENNIZZO;PIVA_ID;RAG_SOC_ID;PIVA_UDD;RAG_SOC_UDD;OM1_ID;OM2_ID;OM3_ID"

    val csv1 = CsvRzg1Metadata("csv1", header, Some("27/10/2022"), Some("1"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda6"), Some("5.00"), Some("10.00"), Some("0.00"))
    val csv2 = CsvRzg1Metadata("csv2", header, Some("27/10/2022"), Some("2"), Some("distr2"), Some("azienda2"), Some("udd2"), Some("azienda7"), Some("0.00"), Some("0.00"), Some("5.00"))
    val csv3 = CsvRzg1Metadata("csv3", header, Some("27/10/2022"), Some("3"), Some("distr3"), Some("azienda3"), Some("udd3"), Some("azienda8"), Some("0.0A"), Some("0.00"), Some("0.00"))
    val csv4 = CsvRzg1Metadata("csv4", header, Some("27/10/2022"), Some("4"), Some("distr4"), Some("azienda4"), Some("udd4"), Some("azienda9"), Some("0.00"), Some("0"), Some("10.00"))
    val csv5 = CsvRzg1Metadata("csv5", header, Some("27/10/2022"), Some("5"), Some("distr5"), Some("azienda5"), Some("udd5"), Some("azienda10"), Some("10.00"), Some("20.00"), Some("-30.00"))
    val csv6 = CsvRzg1Metadata("csv6", header, Some("27/10/2022"), Some("6"), Some("distr6"), Some("azienda6"), Some("udd6"), Some("azienda11"), Some("10.00"), Some("20.00"), Some("30.00"))

    val zipMetadata = Environment.sparkContext.parallelize(Seq(
      (ZipRzg1Metadata(new File(""), 123L, Some(csv1), "udd1", "distr1", "udd1", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(1L, "distr1", "udd1", Some(5.0), Some(10.0), Some(0.0)))),
      (ZipRzg1Metadata(new File(""), 123L, Some(csv2), "udd2", "distr2", "udd2", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File(""), 123L, Some(csv3), "udd3", "distr3", "udd3", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(3L, "distr3", "udd3", Some(0.0), Some(0.0), Some(0.0)))),
      (ZipRzg1Metadata(new File(""), 123L, Some(csv4), "udd4", "distr4", "udd4", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File(""), 123L, Some(csv5), "udd5", "distr5", "udd5", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(5L, "distr5", "udd5", None, Some(20.0), Some(30.0)))),
      (ZipRzg1Metadata(new File(""), 123L, Some(csv6), "udd6", "distr6", "udd6", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(7L, "distr6", "udd6", Some(10.0), Some(20.0), Some(30.0))))
    ))

    val indennizziRules: List[IndennizziRule] = List(
      ruleValidateOM1,                //17
      ruleValidateOM2,                //18
      ruleValidateOM3,                //19
      ruleCheckAtLeastOneOMIsValued,  //23
      ruleValidateIdIndennizzo        //20
    )

    val result = zipMetadata.map({ case (zipMeta, aggregatoTotale) => ValidateIndennizziController.validate(zipMeta, aggregatoTotale, indennizziRules) })
    result.collect.foreach(println)

    Assert.assertEquals(6, result.count)
    Assert.assertEquals(2, result.filter(_._1.isAmmissibile).count)
  }

  def testValidateOM(): Unit = {
    val csv1 = CsvRzg1Metadata("csv1", "", Some("27/10/2022"), Some("1"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda6"), Some("5.00"), Some("10.00"), Some("0.00"))
    val csv2 = CsvRzg1Metadata("csv2", "", Some("27/10/2022"), Some("2"), Some("distr2"), Some("azienda2"), Some("udd2"), Some("azienda7"), Some("0.00"), Some("0.00"), Some("-5.00"))
    val csv3 = CsvRzg1Metadata("csv3", "", Some("27/10/2022"), Some("3"), Some("distr3"), Some("azienda3"), Some("udd3"), Some("azienda8"), Some("0.0A"), Some("0.00"), Some("0.00"))
    val csv4 = CsvRzg1Metadata("csv4", "", Some("27/10/2022"), Some("4"), Some("distr4"), Some("azienda4"), Some("udd4"), Some("azienda9"), Some("00"), Some("10.00"), Some("10.0A"))
    val csv5 = CsvRzg1Metadata("csv5", "", Some("27/10/2022"), Some("5"), Some("distr5"), Some("azienda5"), Some("udd5"), Some("azienda10"), Some("-10.00"), Some("20.00"), Some("-30.00"))
    val csv6 = CsvRzg1Metadata("csv6", "", Some("27/10/2022"), Some("6"), Some("distr6"), Some("azienda6"), Some("udd6"), Some("azienda11"), None, Some("20.00"), Some("-30.001"))

    val zipMetadata = Environment.sparkContext.parallelize(Seq(
      (ZipRzg1Metadata(new File("zip1"), 123L, Some(csv1), "udd1", "distr1", "udd1", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(1L, "distr1", "udd1", Some(5.0), Some(10.0), Some(0.0)))),
      (ZipRzg1Metadata(new File("zip2"), 123L, Some(csv2), "udd2", "distr2", "udd2", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("zip3"), 123L, Some(csv3), "udd3", "distr3", "udd3", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(3L, "distr3", "udd3", Some(0.0), Some(0.0), Some(0.0)))),
      (ZipRzg1Metadata(new File("zip4"), 123L, Some(csv4), "udd4", "distr4", "udd4", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("zip5"), 123L, Some(csv5), "udd5", "distr5", "udd5", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(5L, "distr5", "udd5", None, Some(20.0), Some(30.0)))),
      (ZipRzg1Metadata(new File("zip6"), 123L, Some(csv6), "udd6", "distr6", "udd6", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(6L, "distr6", "udd6", Some(10.0), Some(20.0), Some(30.0))))
    ))

    val resultOM1 = zipMetadata.map({
      case (zipMeta, aggregatoTotale) =>
        (zipMeta.file, ValidateIndennizziController.ruleValidateOM1.condition(zipMeta, aggregatoTotale))
    })
    
    val resultOM2 = zipMetadata.map({
      case (zipMeta, aggregatoTotale) =>
        (zipMeta.file, ValidateIndennizziController.ruleValidateOM2.condition(zipMeta, aggregatoTotale))
    })

    val resultOM3 = zipMetadata.map({
      case (zipMeta, aggregatoTotale) =>
        (zipMeta.file, ValidateIndennizziController.ruleValidateOM3.condition(zipMeta, aggregatoTotale))
    })
    
    resultOM1.collect.foreach(println)
    resultOM2.collect.foreach(println)
    resultOM3.collect.foreach(println)

    Assert.assertEquals(6, resultOM1.count)
    Assert.assertEquals(3, resultOM1.filter(_._2).count)
    Assert.assertEquals(6, resultOM2.count)
    Assert.assertEquals(6, resultOM2.filter(_._2).count)
    Assert.assertEquals(6, resultOM3.count)
    Assert.assertEquals(2, resultOM3.filter(_._2).count)
  }

  def testValidateIdIndennizzo(): Unit = {
    val csv1 = CsvRzg1Metadata("csv1", "", Some("27/10/2022"), Some("1"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda6"), Some("5.0"), Some("10.0"), Some("0.0"))
    val csv2 = CsvRzg1Metadata("csv2", "", Some("27/10/2022"), Some("2"), Some("distr2"), Some("azienda2"), Some("udd2"), Some("azienda7"), Some("0.0"), Some("0.0"), Some("5.0"))
    val csv3 = CsvRzg1Metadata("csv3", "", Some("27/10/2022"), Some("3"), Some("distr3"), Some("azienda3"), Some("udd3"), Some("azienda8"), Some("0.0A"), Some("0.0"), Some("0.0"))
    val csv4 = CsvRzg1Metadata("csv4", "", Some("27/10/2022"), Some("4"), Some("distr4"), Some("azienda4"), Some("udd4"), Some("azienda9"), Some("0"), Some("10.0"), Some("10.0"))
    val csv5 = CsvRzg1Metadata("csv5", "", Some("27/10/2022"), Some("5"), Some("distr5"), Some("azienda5"), Some("udd5"), Some("azienda10"), Some("-10.0"), Some("20.0"), Some("30.0"))
    val csv6 = CsvRzg1Metadata("csv6", "", Some("27/10/2022"), Some("6"), Some("distr6"), Some("azienda6"), Some("udd6"), Some("azienda11"), Some("-10.0"), Some("20.0"), Some("30.0"))

    val zipMetadata = Environment.sparkContext.parallelize(Seq(
      (ZipRzg1Metadata(new File("zip1"), 123L, Some(csv1), "udd1", "distr1", "udd1", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(1L, "distr1", "udd1", Some(5.0), Some(10.0), Some(0.0)))),
      (ZipRzg1Metadata(new File("zip2"), 123L, Some(csv2), "udd2", "distr2", "udd2", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("zip3"), 123L, Some(csv3), "udd3", "distr3", "udd3", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(3L, "distr3", "udd3", Some(0.0), Some(0.0), Some(0.0)))),
      (ZipRzg1Metadata(new File("zip4"), 123L, Some(csv4), "udd4", "distr4", "udd4", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("zip5"), 123L, Some(csv5), "udd5", "distr5", "udd5", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(5L, "distr5", "udd5", None, Some(20.0), Some(30.0)))),
      (ZipRzg1Metadata(new File("zip6"), 123L, Some(csv6), "udd6", "distr6", "udd6", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        Some(AggregatoTotale(0L, "distr6", "udd6", Some(10.0), Some(20.0), Some(30.0))))
    ))

    val result = zipMetadata.map({ case (zipMeta, aggregatoTotale) =>
      (zipMeta.file, ValidateIndennizziController.ruleValidateIdIndennizzo.condition(zipMeta, aggregatoTotale))
    })
    result.collect.foreach(println)

    Assert.assertEquals(6, result.count)
    Assert.assertEquals(4, result.filter(_._2).count)
  }

  def testCheckAtLeastOneOMIsValued(): Unit = {
    val csv1 = CsvRzg1Metadata("csv1", "", Some("27/10/2022"), Some("1"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda6"), Some("5.0"), None, None)
    val csv2 = CsvRzg1Metadata("csv2", "", Some("27/10/2022"), Some("2"), Some("distr2"), Some("azienda2"), Some("udd2"), Some("azienda7"), None, Some("0.0"), None)
    val csv3 = CsvRzg1Metadata("csv3", "", Some("27/10/2022"), Some("3"), Some("distr3"), Some("azienda3"), Some("udd3"), Some("azienda8"), None, None, Some("0.0"))
    val csv4 = CsvRzg1Metadata("csv4", "", Some("27/10/2022"), Some("4"), Some("distr4"), Some("azienda4"), Some("udd4"), Some("azienda9"), None, None, None)

    val zipMetadata = Environment.sparkContext.parallelize(Seq(
      (ZipRzg1Metadata(new File("zip1"), 123L, Some(csv1), "udd1", "distr1", "udd1", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("zip2"), 123L, Some(csv2), "udd2", "distr2", "udd2", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("zip3"), 123L, Some(csv3), "udd3", "distr3", "udd3", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("zip4"), 123L, Some(csv4), "udd4", "distr4", "udd4", "2022", "1027", "202210", "20221027144500", "1", isAmmissibile = true),
        None)
    ))

    val result = zipMetadata.map({ case (zipMeta, aggregatoTotale) =>
      (zipMeta.file, ValidateIndennizziController.ruleCheckAtLeastOneOMIsValued.condition(zipMeta, aggregatoTotale))
    })
    result.collect.foreach(println)

    Assert.assertEquals(4, result.count)
    Assert.assertEquals(3, result.filter(_._2).count)
  }
}
