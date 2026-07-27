package it.eng.au.aggiustamentoGas.utility

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasConnessioniDistr2Schema
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants.TIMESTAMP_FORMAT
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility.filterDfWithStartEndDate
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

import scala.util.Try

class DateUtilityTestEnvironment extends EnvironmentSparkTest {
  def testMonthBetween(): Unit = {
    val date: DateTime = DateTime.parse("03/08/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date2: DateTime = DateTime.parse("05/08/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))

    Assert.assertEquals(0, DateUtility.monthsDifference(date, date2))
    Assert.assertEquals(0, DateUtility.monthsDifference(date2, date))
    Assert.assertEquals(0, DateUtility.monthsDifference(date2, date2))
    Assert.assertEquals(0, DateUtility.monthsDifference(date, date))

    val datePrime: DateTime = DateTime.parse("01/07/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date2Prime: DateTime = DateTime.parse("02/07/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    Assert.assertEquals(0, DateUtility.monthsDifference(datePrime, date2Prime))
    Assert.assertEquals(0, DateUtility.monthsDifference(date2Prime, datePrime))

    val dateJan: DateTime = DateTime.parse("03/01/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val dateMar: DateTime = DateTime.parse("05/03/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    Assert.assertEquals(2, DateUtility.monthsDifference(dateMar, dateJan))
    Assert.assertEquals(2, DateUtility.monthsDifference(dateJan, dateMar))
  }

  def testDaysBetween(): Unit = {
    val date: DateTime = DateTime.parse("03/08/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date2: DateTime = DateTime.parse("05/08/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val date3: DateTime = DateTime.parse("05/03/2020", DateTimeFormat.forPattern("dd/MM/yyyy"))
    Assert.assertEquals(2, DateUtility.daysBetween(date, date2))
    Assert.assertEquals(2, DateUtility.daysBetween(date2, date))
    Assert.assertEquals(0, DateUtility.daysBetween(date2, date2))
    Assert.assertEquals(0, DateUtility.daysBetween(date, date))
    Assert.assertEquals(151, DateUtility.daysBetween(date, date3))
    Assert.assertEquals(151, DateUtility.daysBetween(date3, date))
  }

  def testFilterDfWithStartEndDate(): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val df = List(
      ("1", null, null)
      , ("2", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0")
      , ("3", "2022-02-01 00:00:00.0", "2022-05-01 00:00:00.0")
      , ("4", "2022-01-31 00:00:00.0", "2022-05-01 00:00:00.0")
      , ("5", "2021-10-01 00:00:00.0", "2021-10-31 00:00:00.0")
      , ("6", "2021-10-01 00:00:00.0", "2021-11-01 00:00:00.0")
      , ("7", "2021-05-01 00:00:00.0", "2022-11-01 00:00:00.0")
      , ("8", "2022-01-01 00:00:00.0", "2022-11-01 00:00:00.0")
      , ("9", "2021-01-01 00:00:00.0", "2022-01-01 00:00:00.0")
      , ("10", "2021-01-01 00:00:00.0", null)
      , ("11", null, "2022-01-01 00:00:00.0")
    ).toDF("pdr", "datainizio", "datafine")

    val res = filterDfWithStartEndDate(df
      , "datainizio"
      , "datafine"
      , "yyyy-MM-dd HH:mm:ss.S"
      , "202111"
      , "202201"
      , "yyyyMM"
    ).cache()

    res.show()

    Assert.assertEquals(1, res.filter(col("pdr") === "1").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "2").count())
    Assert.assertEquals(0, res.filter(col("pdr") === "3").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "4").count())
    Assert.assertEquals(0, res.filter(col("pdr") === "5").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "6").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "7").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "8").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "9").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "10").count())
    Assert.assertEquals(1, res.filter(col("pdr") === "11").count())

  }
}
