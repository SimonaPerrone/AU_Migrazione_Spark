package it.eng.au.gse.common.controller

import it.eng.au.gse.common.EnvironmentSparkTest
import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import it.eng.au.gse.common.utility.environment.Environment
import org.junit.Assert

class TransformCommonTest extends EnvironmentSparkTest {
  def testJoinDwhConsumiWithPods(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val dwhConsumi = Seq(
      ("pod1", "05/2023"),
      ("pod2", "05/2023"),
      ("pod2", "06/2023"),
      ("pod3", "07/2023")
    ).toDF(DwhConsumiSchema.pod14, DwhConsumiOutputSchema.meseanno)

    val pods = Seq(
      ("pod1", "05/2023"),
      ("pod2", "05/2023"),
      ("pod3", "05/2023")
    ).toDF(GsePerimetroSchema.t_cod_pod, GsePerimetroSchema.t_mese_anno)

    val output = TransformCommon.joinDwhConsumiWithPods(dwhConsumi, pods)
    output.show

    Assert.assertEquals(2, output.count)
  }
}
