package it.eng.au.portaleConsumi.dao.hive.misure

import it.eng.au.portaleConsumi.EnvironmentSparkTest
import it.eng.au.portaleConsumi.dao.hive.misuregas.FornitureProcessiGasDao
import it.eng.au.portaleConsumi.model.hive.misuregas.FornitureProcessiGasModel
import it.eng.au.portaleConsumi.schema.misuregas.FornitureProcessiGasSchema
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.junit.Assert

class FornitureProcessiGasDaoTest extends EnvironmentSparkTest {
  val spark = Environment.getSpark

  import spark.implicits._

  case class FornitureProcessiGasDaoMock(ds: Dataset[FornitureProcessiGasModel]) extends FornitureProcessiGasDao {
    override def read(columns: List[String]): Dataset[FornitureProcessiGasModel] = ds
  }

  def testCalcolaDeltaSingolaPartizione(): Unit = {
    val fornitura11 = FornitureProcessiGasModel(hashcode = "1", codice_fiscale = "1", codice_pdr = "1", data_calcolo = "2023-01-01")
    val fornitura21 = FornitureProcessiGasModel(hashcode = "2", codice_fiscale = "2", codice_pdr = "2", data_calcolo = "2023-01-01")
    val fornitura31 = FornitureProcessiGasModel(hashcode = "3", codice_fiscale = "3", codice_pdr = "3", data_calcolo = "2023-01-01")
    val ds = Seq(fornitura11, fornitura21, fornitura31).toDS()

    val result = FornitureProcessiGasDaoMock(ds).calcolaDelta().cache()

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(fornitura11, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 1).collect().head)
    Assert.assertEquals(fornitura21, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 2).collect().head)
    Assert.assertEquals(fornitura31, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 3).collect().head)
  }

  def testCalcolaDeltaDuePartizioni(): Unit = {
    val fornitura11 = FornitureProcessiGasModel(hashcode = "1", codice_fiscale = "1", codice_pdr = "1", data_calcolo = "2023-01-01")
    val fornitura12 = FornitureProcessiGasModel(hashcode = "1", codice_fiscale = "1", codice_pdr = "1", data_calcolo = "2023-01-02")
    val fornitura21 = FornitureProcessiGasModel(hashcode = "2", codice_fiscale = "2", codice_pdr = "2", data_calcolo = "2023-01-01")
    val fornitura22 = FornitureProcessiGasModel(hashcode = "22", codice_fiscale = "2", codice_pdr = "2", data_calcolo = "2023-01-02")
    val fornitura31 = FornitureProcessiGasModel(hashcode = "3", codice_fiscale = "3", codice_pdr = "3", data_calcolo = "2023-01-01")
    val fornitura41 = FornitureProcessiGasModel(hashcode = "4", codice_fiscale = "3", codice_pdr = "4", data_calcolo = "2023-01-02")
    val ds = Seq(fornitura11, fornitura12, fornitura21, fornitura22, fornitura31, fornitura41).toDS()

    val result = FornitureProcessiGasDaoMock(ds).calcolaDelta().cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(0, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 1).count())
    Assert.assertEquals(0, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 3).count())
    Assert.assertEquals(1, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 2).count())
    Assert.assertEquals(fornitura22, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 2).collect().head)
    Assert.assertEquals(fornitura41, result.where(col(FornitureProcessiGasSchema.codice_pdr) === 4).collect().head)
  }

  def testCalcolaDeltaDoppiaFornitura(): Unit = {
    val fornitura1 = FornitureProcessiGasModel(hashcode = "1", codice_fiscale = "1", codice_pdr = "1", data_calcolo = "2023-01-01")
    val fornitura11 = FornitureProcessiGasModel(hashcode = "1", codice_fiscale = "1", codice_pdr = "1", data_calcolo = "2023-01-02")
    val fornitura12 = FornitureProcessiGasModel(hashcode = "2", codice_fiscale = "1", codice_pdr = "2", data_calcolo = "2023-01-02")
    val ds = Seq(fornitura1, fornitura11, fornitura12).toDS()

    val result = FornitureProcessiGasDaoMock(ds).calcolaDelta().cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(1, result.where(col(FornitureProcessiGasSchema.codice_pdr) === "1").count())
    Assert.assertEquals(1, result.where(col(FornitureProcessiGasSchema.codice_pdr) === "2").count())
    Assert.assertEquals(2, result.where(col(FornitureProcessiGasSchema.codice_fiscale) === "1").count())
  }

  def testDateCalcolo(): Unit = {
    val data1 = "2022-01-01"
    val data2 = "2023-03-01"
    val data3 = "2023-01-02"
    val ds = Seq(
      FornitureProcessiGasModel(data_calcolo = data1),
      FornitureProcessiGasModel(data_calcolo = data2),
      FornitureProcessiGasModel(data_calcolo = data3)
    ).toDS()

    val expected = List(data2, data3, data1)
    val result = FornitureProcessiGasDaoMock(ds).dateCalcolo()

    Assert.assertEquals(expected, result)
  }

  def testCancellaDatiPrecedentiUltimaEsecuzione(): Unit = {
    val data1 = "2022-01-01"
    val data2 = "2023-03-01"
    val data3 = "2023-01-02"
    val ds = Seq(
      FornitureProcessiGasModel(data_calcolo = data1),
      FornitureProcessiGasModel(data_calcolo = data2),
      FornitureProcessiGasModel(data_calcolo = data3)
    ).toDS()

    val expected = s"""ALTER TABLE ${new FornitureProcessiGasDao().tableName} DROP PARTITION (${FornitureProcessiGasSchema.data_calcolo}="$data2"), PARTITION (${FornitureProcessiGasSchema.data_calcolo}="$data3")"""
    val result = FornitureProcessiGasDaoMock(ds).comandoCancellazionePartizioni(List(data1, data2, data3))

    Assert.assertEquals(expected, result)
  }

}
