package it.eng.au.gse.calcoloMensile.controller

import it.eng.au.gse.calcoloMensile.EnvironmentSparkTest
import it.eng.au.gse.calcoloMensile.schema.gse.GseRichiestaSchema
import it.eng.au.gse.common.controller.PrepareCommon
import it.eng.au.gse.common.schema.dwh.DwhConsumiSchema
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

    Assert.assertEquals(6, output.count)
    Assert.assertEquals(2, output.where(col(GsePerimetroSchema.t_cod_pod) === "pod3").count)
  }

  def testPrepareRequests(): Unit = {
    val yearMonth = YearMonth.parse("2023-05")
    Environment.setProperty("year.month", yearMonth.format(DateTimeFormatter.ofPattern("MM/yyyy")))

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val requests = Seq(
      ("1", "N", "03/2023", "pod1", "cf1"),
      ("2", "N", "04/2023", "pod2", "cf1"),
      ("2", "N", "04/2023", "pod2", "piva1"),
      ("3", "N", "05/2023", "pod3", "piva1"),
      ("4", "S", "05/2023", "pod4", "cf1"),
      ("4", "S", "05/2023", "pod4", "piva1"),
      ("5", "S", "05/2023", null, "cf1"),
      ("5", "S", "05/2023", null, "piva1"),
      ("6", "N", "05/2023", null, null),
      ("7", "N", null, "pod7", null),
      ("8", "N", "malformed", "pod7", null)
    ).toDF(GseRichiestaSchema.getValues: _*)

    val (newRequests, yearMonthList) = Prepare.prepareRequests(requests)
    newRequests.show
    println(yearMonthList.mkString("\n"))

    Assert.assertEquals(7, newRequests.count)
    Assert.assertEquals(1, newRequests.where(col(GseRichiestaSchema.n_id_gse_richiesta_er_m) === "6").count)
    Assert.assertEquals(2, newRequests.where(col(GseRichiestaSchema.n_id_gse_richiesta_er_m) === "2").count)
    Assert.assertEquals(3, yearMonthList.size)
  }
}
