package it.eng.au.gse.calcoloAnnuale.controller

import it.eng.au.gse.calcoloAnnuale.EnvironmentSparkTest
import it.eng.au.gse.calcoloAnnuale.schema.GseRichiestaSchema
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.time.YearMonth
import java.time.format.DateTimeFormatter

class PrepareTest extends EnvironmentSparkTest {
  def testPreparePodPerimeter(): Unit = {
    val yearMonth = YearMonth.parse("2023-05")
    Environment.setProperty("year.month", yearMonth.format(DateTimeFormatter.ofPattern("MM/yyyy")))
    Environment.setProperty("year", yearMonth.getYear.toString)

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val podPerimeter = Seq(
      ("pod1", "03/2023", "cf1", "piva1", "Y"),
      ("pod2", "04/2023", "cf1", "piva1", "Y"),
      ("pod3", "05/2023", "cf1", "piva1", "Y"),
      ("pod4", "05/2023", "cf1", "piva1", "N"),
      ("pod5", "05/2023", "cf1", "piva1", "X")
    ).toDF(GsePerimetroSchema.getValues: _*)

    val output = Prepare.preparePodPerimeter(podPerimeter)
    output.show

    Assert.assertEquals(3, output.count)
    Assert.assertEquals(1, output.where(col(GsePerimetroSchema.t_cod_pod) === "pod3").count)
  }

  def testPrepareRequests(): Unit = {
    val yearMonth = YearMonth.parse("2023-05")
    Environment.setProperty("year.month", yearMonth.format(DateTimeFormatter.ofPattern("MM/yyyy")))
    Environment.setProperty("year", yearMonth.getYear.toString)

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val requests = Seq(
      ("1", "N", Some("2021")),
      ("2", "N", Some("2021")),
      ("2", "N", Some("2021")),
      ("3", "N", Some("2021")),
      ("4", "S", Some("2023")),
      ("4", "S", Some("2023")),
      ("5", "S", Some("2023")),
      ("5", "S", Some("2023")),
      ("6", "N", Some("2023")),
      ("7", "N", None),
      ("8", "N", Some("malformed"))
    ).toDF(GseRichiestaSchema.getValues: _*)

    val (newRequests, yearMonthList) = Prepare.prepareRequests(requests)
    newRequests.show
    println(yearMonthList.mkString("\n"))

    Assert.assertEquals(7, newRequests.count)
    Assert.assertEquals(1, newRequests.where(col(GseRichiestaSchema.n_id_gse_richiesta_er_a) === "6").count)
    Assert.assertEquals(2, newRequests.where(col(GseRichiestaSchema.n_id_gse_richiesta_er_a) === "2").count)
    Assert.assertEquals(24, yearMonthList.size)
  }
}
