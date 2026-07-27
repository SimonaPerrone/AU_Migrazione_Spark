package it.eng.au.CalcoloSettlementGas.controller

import it.eng.au.CalcoloSettlementGas.utility.EnvironmentSparkTest
import it.eng.au.calcoloSettlementGas.controller.CalcoloController
import it.eng.au.calcoloSettlementGas.dao.TabParametriCaratteristiciProfPrelDao
import it.eng.au.calcoloSettlementGas.schema.AtgTabProfiliGiornStdPercSchema
import it.eng.au.calcoloSettlementGas.utility.Constants.ID_REG_CLIM_VALUES_Complete
import it.eng.au.calcoloSettlementGas.utility.Properties
import it.eng.au.trasmissioneSettlementGasCommon.schema.{TSGTFCSchema, TSGVPGSchema}
import it.eng.au.trasmissioneSettlementGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.io.File

class CalcoloControllerTest extends EnvironmentSparkTest {
  def testGetLatestVPGRecords(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = Environment.spark.sparkContext.parallelize(Seq(
      ("1", "01/01/2023", "10000001", "1"),
      ("2", "01/01/2023", "10000000", "1"),

      ("3", "02/01/2023", "10000000", "1"),
      ("4", "02/01/2023", "10000000", "2"),

      ("5", "03/01/2023", "10000000", "1"),
      ("6", "03/01/2023", "10000000", "1"),

      ("7", "04/01/2023", "10000000", "1"),

      ("8", "05/01/2023", "10000000", "1")
    )).toDF(TSGVPGSchema.n_id_TSG2_file, TSGVPGSchema.giorno_riferimento, TSGVPGSchema.executionid, TSGVPGSchema.progressivo)

    val result = CalcoloController.getLatestVPGRecords(df)
    result.sort(col(TSGVPGSchema.giorno_riferimento).asc).show()

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(0, result.where(col(TSGVPGSchema.n_id_TSG2_file) === "2").count)
    Assert.assertEquals(1, result.where(col(TSGVPGSchema.n_id_TSG2_file) === "4").count)
    Assert.assertEquals(1, result.where(col(TSGVPGSchema.n_id_TSG2_file) === "7").count)
  }

  def testGetLatestTFCRecords(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = Environment.spark.sparkContext.parallelize(Seq(
      ("1", "01/01/2023", "11", "10000000", "1"),
      ("2", "01/01/2023", "12", "10000001", "1"),
      ("3", "01/01/2023", "13", "10000000", "1"),
      ("4", "01/01/2023", "13", "10000000", "2"),

      ("5", "02/01/2023", "11", "10000000", "1"),
      ("6", "02/01/2023", "12", "10000000", "1"),
      ("7", "02/01/2023", "12", "10000000", "2"),

      ("8", "03/01/2023", "11", "10000000", "1"),
      ("9", "03/01/2023", "12", "10000000", "1"),
      ("10", "03/01/2023", "13", "10000000", "1"),

      ("11", "04/01/2023", "11", "10000000", "1"),

      ("12", "05/01/2023", "11", "10000000", "1")
    )).toDF(TSGTFCSchema.n_id_TSG2_file, TSGTFCSchema.data, TSGTFCSchema.id_reg_clim, TSGTFCSchema.executionid, TSGTFCSchema.progressivo)

    val result = CalcoloController.getLatestTFCRecords(df.filter(col(TSGTFCSchema.id_reg_clim) === "11"))
    result.sort(col(TSGTFCSchema.data).asc).show()

    Assert.assertEquals(5, result.count)
    Assert.assertEquals(1, result.where(col(TSGTFCSchema.n_id_TSG2_file) === "1").count)
    Assert.assertEquals(1, result.where(col(TSGTFCSchema.n_id_TSG2_file) === "5").count)
    Assert.assertEquals(1, result.where(col(TSGTFCSchema.n_id_TSG2_file) === "8").count)
  }

  def testGetParametriCarattProfPrelFromFile(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val file = new File("src/main/resources/input/Tabella_Generale_Codici_Profili_Standard.csv")
    val parametriCarattProfPrelList = CalcoloController.getParametriCarattProfPrelFromFile(file)


    println(parametriCarattProfPrelList)
  }

  def testProva(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    println(Properties.getTSG2TabProfiliGiornStdPercPath)
    println(Properties.getTSG2AtgTabProfiliGiornStdPercTableName)
    println(AtgTabProfiliGiornStdPercSchema.getValues)
  }


  def testTabParametriCaratteristiciProfPrelDaoGet(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val file = new File("src/main/resources/input/Tabella_Generale_Codici_Profili_Standard.csv")
    val parametriCarattProfPrelList = CalcoloController.getParametriCarattProfPrelFromFile(file)

    val dao = new TabParametriCaratteristiciProfPrelDao()

    val df = dao.get(parametriCarattProfPrelList)

    df.show(false)
    println(df.count())

  }

  def testCalcolo(): Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    val dfIdRegClim = Environment.spark.createDataFrame(Environment.sparkContext.parallelize(ID_REG_CLIM_VALUES_Complete)).toDF(TSGTFCSchema.id_reg_clim, "Nome regione")
      .select(TSGTFCSchema.id_reg_clim)

    //dfIdRegClim.show()
  }
}

/*class TFCControllerTest extends EnvironmentSparkTest {
  val path = "src/test/resources/input"//TSG2_10238291008"

  def testReadAndCheckCsvFiles():Unit = {
    val sc = Environment.sparkContext
    sc.setLogLevel("ERROR")

    Environment.setProperty("csv.input.path", path)
    //Environment.setProperty("current.year", "2021")
    Environment.setProperty("current.year", "2021")
    //Environment.setProperty("current.month", "05")
    Environment.setProperty("current.month", "10") */
