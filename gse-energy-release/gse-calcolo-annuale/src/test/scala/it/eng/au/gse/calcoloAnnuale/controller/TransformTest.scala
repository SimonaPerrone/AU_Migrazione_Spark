package it.eng.au.gse.calcoloAnnuale.controller

import it.eng.au.gse.calcoloAnnuale.EnvironmentSparkTest
import it.eng.au.gse.calcoloAnnuale.schema.{GseAggrASchema, GseRichiestaSchema}
import it.eng.au.gse.common.dao.GsePerimetroDao
import it.eng.au.gse.common.schema.dwh.DwhConsumiOutputSchema
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.time.format.DateTimeFormatter

class TransformTest extends EnvironmentSparkTest {
  def testJoinPerimeterAndRequests(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val perimeter = Seq(
      ("pod1", "05/2021", "2021", "cf1", "Y"),
      ("pod1", "05/2021", "2021", "piva1", "Y"),
      ("pod2", "05/2021", "2021", "cf2", "Y"),
      ("pod2", "05/2021", "2021", "piva2", "Y"),
      ("pod3", "05/2021", "2021", "cf35", "Y"),
      ("pod3", "05/2021", "2021", "piva35", "Y"),
      ("pod3", "06/2021", "2021", "cf36", "Y"),
      ("pod3", "06/2021", "2021", "piva36", "Y"),
      ("pod4", "05/2021", "2021", "cf1", "Y"),
      ("pod4", "05/2021", "2021", "piva1", "Y"),
      ("pod5", "05/2021", "2021", "cf2", "Y"),
      ("pod5", "05/2022", "2022", "piva2", "Y"),
      ("pod6", "05/2022", "2022", "cf3", "Y"),
      ("pod6", "05/2022", "2022", "piva3", "Y"),
      ("pod7", "05/2022", "2022", "cf1", "Y"),
      ("pod7", "05/2022", "2022", "piva1", "Y"),
      ("pod8", "05/2022", "2022", "cf2", "Y"),
      ("pod8", "05/2022", "2022", "piva2", "Y"),
      ("pod9", "05/2022", "2022", "cf3", "Y"),
      ("pod9", "05/2022", "2022", "piva3", "Y"),
      ("pod10", "05/2023", "2023", "cf1", "Y"),
      ("pod10", "05/2023", "2023", "piva1", "Y"),
      ("pod11", "05/2023", "2023", "cf2", "Y"),
      ("pod11", "05/2023", "2023", "piva2", "Y"),
      ("pod12", "06/2023", "2023", "cf3", "Y"),
      ("pod12", "06/2023", "2023", "piva3", "Y"),
      ("pod13", "05/2023", "2023", "cf1", "Y"),
      ("pod13", "05/2023", "2023", "piva1", "Y"),
      ("pod14", "05/2023", "2023", "cf2", "Y"),
      ("pod14", "05/2023", "2023", "piva2", "Y"),
      ("pod15", "06/2023", "2023", "cf3", "Y"),
      ("pod15", "06/2023", "2023", "piva3", "Y"),
      ("pod16", "05/2023", "2023", "cf1", "Y"),
      ("pod16", "05/2023", "2023", "piva1", "Y"),
      ("pod17", "05/2023", "2023", "cf2", "Y"),
      ("pod17", "05/2023", "2023", "piva2", "Y"),
      ("pod18", "06/2023", "2023", "cf3", "Y"),
      ("pod18", "06/2023", "2023", "piva3", "Y")
    ).toDF(GsePerimetroSchema.t_cod_pod,
      GsePerimetroSchema.t_mese_anno,
      GsePerimetroDao.t_anno,
      GsePerimetroDao.t_cliente,
      GsePerimetroSchema.t_valido)

    val newRequests = Seq(
      ("1", "N", "2021"),
      ("2", "N", "2021"),
      ("3", "N", "2021"),
      ("4", "N", "2022"),
      ("5", "N", "2022"),
      ("6", "N", "2022"),
      ("7", "N", "2023"),
      ("8", "N", "2023")
    ).toDF(GseRichiestaSchema.getValues: _*)

    val (requests, pods) = Transform.joinPerimeterAndRequests(perimeter, newRequests)

    requests.show()
    pods.show()

    Assert.assertEquals(51, requests.count)
    Assert.assertEquals(20, pods.count)
    Assert.assertEquals(2, pods.where(col(GsePerimetroSchema.t_cod_pod) === "pod3").count())
  }

  def testJoinRequestsAndConsumptions(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val executionId = Environment.executionId
    val startDate = Environment.startDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val requests = Seq(
      ("1", "pod1", "04/2023"),
      ("2", "pod1", "05/2023"),
      ("3", "pod2", "05/2023"),
      ("4", "pod2", "06/2023"),
      ("5", "pod3", "07/2023")
    ).toDF(GseRichiestaSchema.n_id_gse_richiesta_er_a, GsePerimetroSchema.t_cod_pod, GsePerimetroSchema.t_mese_anno)

    val consumptions = Seq(
      ("pod1", 12.0, "05/2023"),
      ("pod2", 12.0, "05/2023"),
      ("pod3", 12.0, "07/2023")
    ).toDF(DwhConsumiOutputSchema.codice_pod, DwhConsumiOutputSchema.consumo, DwhConsumiOutputSchema.meseanno)

    val output = Transform.joinRequestsAndConsumptions(requests, consumptions, startDate, executionId)
    output.show

    Assert.assertEquals(5, output.count)
    Assert.assertEquals(2, output.where(col(GseAggrASchema.t_cod_pod) === "pod1").count)
    Assert.assertEquals(2, output.where(col(GseAggrASchema.t_cod_pod) === "pod2").count)
    Assert.assertEquals(1, output.where(col(GseAggrASchema.t_cod_pod) === "pod2" && col(GseAggrASchema.n_consumo_mensile).isNotNull).count)
  }
}
