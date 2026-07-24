package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.controller.ReadCsvController._
import it.eng.au.ammissibilitaRendiconti.model.rules.{CsvRule, ZipRule}
import it.eng.au.ammissibilitaRendiconti.model.{CsvRzg1Metadata, ZipRzg1Metadata}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.junit.Assert

import java.io.File

class ReadCsvControllerTest extends EnvironmentSparkTest {
  def testReadCsv(): Unit = {
    val correctZipPath = "src/test/resources/input/read_csv_controller/read_csv/CIG2_distr1/2022/11/distr1_udd1_correct_case.zip"
    val badBodyZip = "src/test/resources/input/read_csv_controller/read_csv/CIG2_distr1/2022/11/bad_zip_body.zip"
    val badNameZip = "src/test/resources/input/read_csv_controller/read_csv/CIG2_distr1/2022/11/bad_zip_name.zip"
    val badCsvZip = "src/test/resources/input/read_csv_controller/read_csv/CIG2_distr1/2022/11/empty_csv.zip"
    val badCsvHeaderZip = "src/test/resources/input/read_csv_controller/read_csv/CIG2_distr1/2022/11/bad_csv_header.zip"

    val zipMeta = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File(correctZipPath), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(badBodyZip), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(badNameZip), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(badCsvZip), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(badCsvHeaderZip), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)
    ))

    val zipRules: List[ZipRule] = List(
      ruleCheckZipIntegrity,    //3
      ruleCheckZipBody,         //4
      ruleCheckZipAndCsvNames,  //5
      ruleCheckPiva,            //6
      ruleCheckCsvEncoding)     //7

    val csvRules: List[CsvRule] = List(
      ruleCheckHeader,              //10
      ruleCheckDate,                //11
      ruleCheckIdIndennizzo,        //12
      ruleCheckPivaId,              //13
      ruleCheckPivaUdd,             //14
      ruleCheckRagSocId,            //15
      ruleCheckRagSocUdd,           //16
      ruleCheckPivaIdConsistency,   //21
      ruleCheckPivaUddConsistency   //22
    )

    val result = zipMeta.map(ReadCsvController.checkAndUnzip(_, zipRules, csvRules, isRuleCheckNumberOfCsvFieldsEnabled = true))
    result.collect.foreach(println)

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(1, result.filter(_.isAmmissibile).count)
  }

  def testCheckIntegrity(): Unit = {
    val zipMeta = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File("src/test/resources/input/read_csv_controller/check_integrity/bad_integrity.zip"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("src/test/resources/input/read_csv_controller/check_integrity/integrity_ok.zip"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)
    ))

    val result = zipMeta.map(zip => (zip.file, ReadCsvController.ruleCheckZipIntegrity.condition(zip)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckZipBody(): Unit = {
    val correctZipPath = "src/test/resources/input/read_csv_controller/check_body/correct_zip.zip"
    val correctZip1 = ZipRzg1Metadata(new File(correctZipPath), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val badZipPath1 = "src/test/resources/input/read_csv_controller/check_body/bad_zip1.zip"
    val badZip1 = ZipRzg1Metadata(new File(badZipPath1), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val badZipPath2 = "src/test/resources/input/read_csv_controller/check_body/bad_zip2.zip"
    val badZip2 = ZipRzg1Metadata(new File(badZipPath2), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val badZipPath3 = "src/test/resources/input/read_csv_controller/check_body/bad_zip3.zip"
    val badZip3 = ZipRzg1Metadata(new File(badZipPath3), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val badZipPath4 = "src/test/resources/input/read_csv_controller/check_body/bad_zip4.zip"
    val badZip4 = ZipRzg1Metadata(new File(badZipPath4), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val rdd = Environment.sparkContext.parallelize(Seq(
      correctZip1, badZip1, badZip2, badZip3, badZip4
    ))

    val result = rdd.map(zip => (zip.file, ReadCsvController.ruleCheckZipBody.condition(zip)))
    result.collect.foreach(println)

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckPiva(): Unit = {
    val zipMeta = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File("src/test/resources/input/read_csv_controller/CIG1_distr1/2022/11/test.zip"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("src/test/resources/input/read_csv_controller/CIG1_distr2/2022/11/test.zip"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)
    ))

    val result = zipMeta.map(zip => (zip.file, ReadCsvController.ruleCheckPiva.condition(zip)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckCsvEncoding(): Unit = {
    val zipMeta = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File("src/test/resources/input/read_csv_controller/check_encoding/utf8_encoded.zip"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File("src/test/resources/input/read_csv_controller/check_encoding/not_utf8_encoded.zip"), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)
    ))

    val result = zipMeta.map(zip => (zip.file, ReadCsvController.ruleCheckCsvEncoding.condition(zip)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckZipAndCsvNames(): Unit = {
    val correctZipPath = "src/test/resources/input/read_csv_controller/check_csv_name/correct_zip.zip"
    val correctZip = ZipRzg1Metadata(new File(correctZipPath), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val badZipPath = "src/test/resources/input/read_csv_controller/check_csv_name/bad_zip.zip"
    val badZip = ZipRzg1Metadata(new File(badZipPath), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val rdd = Environment.sparkContext.parallelize(Seq(correctZip, badZip))

    val result = rdd.map(zip => (zip.file, ReadCsvController.ruleCheckZipAndCsvNames.condition(zip)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testUnzipCsv(): Unit = {
    val correctZipPath = "src/test/resources/input/read_csv_controller/unzip_csv/correct_zip.zip"
    val correctZip = ZipRzg1Metadata(new File(correctZipPath), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val badZipPath = "src/test/resources/input/read_csv_controller/unzip_csv/bad_zip.zip"
    val badZip = ZipRzg1Metadata(new File(badZipPath), 123L, None, "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val rdd = Environment.sparkContext.parallelize(Seq(correctZip, badZip))

    val result = rdd.map(ReadCsvController.unzipCsv(_, isRuleCheckNumberOfCsvFieldsEnabled = true))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_.isAmmissibile).count)
  }

  def testCheckHeader(): Unit = {
    val correctHeader = "DATA;ID_INDENNIZZO;PIVA_ID;RAG_SOC_ID;PIVA_UDD;RAG_SOC_UDD;€OM1_ID;€OM2_ID;€OM3_ID"
    val correctCsv = CsvRzg1Metadata("", correctHeader, Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val correctZip = ZipRzg1Metadata(new File(""), 123L, Some(correctCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)

    val badHeader = "DATA;ID_INDENNIZZO;PIVA_ID;RAG_SOC_ID;PIVA_UDD;RAG_SOC_UDD;OM1_ID;OM2_ID;OM3_ID"
    val badCsv = CsvRzg1Metadata("", badHeader, Some("27/10/2022"), Some("12346"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badZip = ZipRzg1Metadata(new File(""), 123L, Some(badCsv), "udd1", "distr1", "udd1", "2022", "10","202210", "20221027161000", "1", isAmmissibile = true)

    val zipMeta = Environment.sparkContext.parallelize(Seq(correctZip, badZip))

    val result = zipMeta.map(zipMeta => (zipMeta.csv.map(_.header), ReadCsvController.ruleCheckHeader.condition(zipMeta.csv.get)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckDate(): Unit = {
    val correctCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv1 = CsvRzg1Metadata("", "", Some("2022-10-27"), Some("12346"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv2 = CsvRzg1Metadata("", "", Some("10/27/2022"), Some("12346"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv1, badCsv2))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckDate.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckIdIndennizzo(): Unit = {
    val correctCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv1 = CsvRzg1Metadata("", "", Some("27/10/2022"), Some(""), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv2 = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111"),
      Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv1, badCsv2))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckIdIndennizzo.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckPivaId(): Unit = {
    val correctCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv1 = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12346"), Some(""), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv2 = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12346"), Some("distr12345678901234"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv1, badCsv2))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckPivaId.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckPivaUdd(): Unit = {
    val correctCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv1 = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12346"), Some("distr1"), Some("azienda1"), Some(""), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv2 = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12346"), Some("distr1"), Some("azienda1"), Some("udd12345678901234"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv1, badCsv2))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckPivaUdd.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckRagSocId(): Unit = {
    val correctCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12346"), Some("distr1"), Some(""), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckRagSocId.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckRagSocUdd(): Unit = {
    val correctCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv = CsvRzg1Metadata("", "", Some("27/10/2022"), Some("12346"), Some("distr1"), Some("azienda1"), Some("udd1"), Some(""), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckRagSocUdd.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckPivaIdConsistency(): Unit = {
    val correctCsv = CsvRzg1Metadata("distr1_udd1_202208_RZG1_20221128122756_1", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv = CsvRzg1Metadata("distr2_udd2_202208_RZG1_20221128122756_1", "", Some("27/10/2022"), Some("12346"), Some("distr1"), Some("azienda1"), Some("udd2"), Some(""), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckPivaIdConsistency.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }

  def testCheckPivaUddConsistency(): Unit = {
    val correctCsv = CsvRzg1Metadata("distr1_udd1_202208_RZG1_20221128122756_1", "", Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badCsv = CsvRzg1Metadata("distr2_udd2_202208_RZG1_20221128122756_1", "", Some("27/10/2022"), Some("12346"), Some("distr1"), Some("azienda1"), Some("udd1"), Some(""), Some("0.0"), Some("0.0"), Some("0.0"))

    val csvRdd = Environment.sparkContext.parallelize(Seq(correctCsv, badCsv))

    val result = csvRdd.map(csv => (csv, ReadCsvController.ruleCheckPivaUddConsistency.condition(csv)))
    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)
    Assert.assertEquals(1, result.filter(_._2).count)
  }


  def testCheckCsv(): Unit = {
    val correctHeader = "DATA;ID_INDENNIZZO;PIVA_ID;RAG_SOC_ID;PIVA_UDD;RAG_SOC_UDD;€OM1_ID;€OM2_ID;€OM3_ID"
    val badHeader = "DATA;ID_INDENNIZZO;PIVA_ID;RAG_SOC_ID;PIVA_UDD;RAG_SOC_UDD;OM1;OM2;OM3"

    val correctCsv = CsvRzg1Metadata("", correctHeader, Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badHeaderCsv = CsvRzg1Metadata("", badHeader, Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badDateCsv = CsvRzg1Metadata("", correctHeader, Some("27-10-2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val badDateCsv1 = CsvRzg1Metadata("", correctHeader, Some("2022-10-27"), Some("12345"), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val emptyIdIndennizzoCsv = CsvRzg1Metadata("", correctHeader, Some("27/10/2022"), Some(""), Some("distr1"), Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val emptyPivaIdCsv = CsvRzg1Metadata("", correctHeader, Some("27/10/2022"), Some("12345"), None, Some("azienda1"), Some("udd1"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val emptyPivaUddCsv = CsvRzg1Metadata("", correctHeader, Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some(""), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))
    val tooLongPivaUddCsv = CsvRzg1Metadata("", correctHeader, Some("27/10/2022"), Some("12345"), Some("distr1"), Some("azienda1"), Some("thisIsTooLongToBeConsideredAsCorrect"), Some("azienda2"), Some("0.0"), Some("0.0"), Some("0.0"))

    val zipMeta = Environment.sparkContext.parallelize(Seq(
      ZipRzg1Metadata(new File(""), 123L, Some(correctCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123L, Some(badHeaderCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123L, Some(badDateCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123L, Some(badDateCsv1), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123L, Some(emptyIdIndennizzoCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123L, Some(emptyPivaIdCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123L, Some(emptyPivaUddCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true),
      ZipRzg1Metadata(new File(""), 123L, Some(tooLongPivaUddCsv), "udd1", "distr1", "udd1", "2022", "10", "202210", "20221027161000", "1", isAmmissibile = true)
    ))

    val csvRules: List[CsvRule] = List(
      ruleCheckHeader,          //10
      ruleCheckDate,            //11
      ruleCheckIdIndennizzo,    //12
      ruleCheckPivaId,          //13
      ruleCheckPivaUdd,         //14
      ruleCheckRagSocId,        //15
      ruleCheckRagSocUdd        //16
    )

    val result = zipMeta.map(ReadCsvController.checkCsv(_, csvRules))
    result.collect.foreach(println)

    Assert.assertEquals(8, result.count)
    Assert.assertEquals(1, result.filter(_.isAmmissibile).count)
  }
}