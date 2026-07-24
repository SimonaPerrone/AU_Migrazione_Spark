package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.dao.IndennizziRzg2DAO.computeDelta
import it.eng.au.ammissibilitaRendiconti.model.{AggregatoTotale, CsvRzg1Metadata, ZipRzg1Metadata}
import it.eng.au.indennizziMisureGasCommon.schema.IndennizziRzg2Schema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.io.File

class IndennizziRzg2Test extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val csvFile1 = CsvRzg1Metadata("csvFile1", "header", Some("07/11/2022"), Some("1"), Some("id1"), Some("azienda1"), Some("udd1"), Some("azienda6"), Some("0.0"), Some("0.0"), Some("1.0"))
    val csvFile2 = CsvRzg1Metadata("csvFile2", "header", Some("07/11/2022"), Some("2"), Some("id2"), Some("azienda2"), Some("udd2"), Some("azienda7"), Some("1.0"), Some("0.0"), Some("0.0"))
    val csvFile3 = CsvRzg1Metadata("csvFile3", "header", Some("07/11/2022"), Some("3"), Some("id3"), Some("azienda3"), Some("udd3"), Some("azienda8"), Some("2.0"), Some("1.0"), Some("0.0"))
    val csvFile4 = CsvRzg1Metadata("csvFile4", "header", Some("07/11/2022"), Some("4"), Some("id4"), Some("azienda4"), Some("udd4"), Some("azienda9"), Some("3.0"), Some("2.0"), Some("0.0"))
    val csvFile5 = CsvRzg1Metadata("csvFile5", "header", Some("07/11/2022"), Some("5"), Some("id5"), Some("azienda5"), Some("udd5"), Some("azienda10"), Some("4.0"), Some("3.0"), Some("0.0"))

    val zipMetaData = Environment.sparkContext.parallelize(Seq(
      (ZipRzg1Metadata(new File("path/zipfile1_1"), 123L, Some(csvFile1), "udd1", "id1", "udd1", "2022", "11", "202211", "20221107153100", "1", isAmmissibile = true),
        Option(AggregatoTotale(1L, "id1", "udd1", Some(0.0), Some(0.0), Some(0.0)))),
      (ZipRzg1Metadata(new File("path/zipfile1_2"), 123L, Some(csvFile1), "udd1", "id1", "udd1", "2022", "11", "202211", "20221107153100", "2", isAmmissibile = true),
        Option(AggregatoTotale(1L, "id1", "udd1", Some(0.0), Some(0.0), Some(0.0)))),
      (ZipRzg1Metadata(new File("path/zipfile2"), 123L, Some(csvFile2), "udd2", "id2", "udd2", "2022", "11", "202211", "20221107153200", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("path/zipfile3"), 123L, Some(csvFile3), "udd3", "id3", "udd3", "2022", "11", "202211", "20221107153300", "1", isAmmissibile = true),
        Option(AggregatoTotale(3L, "id3", "udd3", Some(-5), Some(-10), Some(-15)))),
      (ZipRzg1Metadata(new File("path/zipfile4"), 123L, Some(csvFile4), "udd4", "id4", "udd4", "2022", "11", "202211", "20221107153400", "1", isAmmissibile = true),
        None),
      (ZipRzg1Metadata(new File("path/zipfile5"), 123L, Some(csvFile5), "udd5", "id5", "udd5", "2022", "11", "202211", "20221107153500", "1", isAmmissibile = false, "505", "not supported"),
        Option(AggregatoTotale(5L, "id5", "udd5", Some(0.0), None, None))),
      (ZipRzg1Metadata(new File("path/zipfile6"), 123L, None, "udd6", "id6", "udd6", "2022", "11", "202211", "20221107153600", "1", isAmmissibile = false, "404", "not found"),
        None)
    ))

    val indennizzirzg2DAO = new IndennizziRzg2DAO()
    val result = indennizzirzg2DAO.get(zipMetaData)
    result.show

    Assert.assertEquals(7, result.count)
    Assert.assertEquals(29, result.columns.length)
    Assert.assertEquals(5, result.where(col(IndennizziRzg2Schema.ammissibilita) === true).count)
    Assert.assertEquals(1, result.where(col(IndennizziRzg2Schema.csv_file_name).isNull).count)
  }
}
