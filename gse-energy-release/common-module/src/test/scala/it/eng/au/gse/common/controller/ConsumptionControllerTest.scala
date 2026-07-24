package it.eng.au.gse.common.controller

import it.eng.au.gse.common.EnvironmentSparkTest
import it.eng.au.gse.common.schema.dwh.{DwhConsumiOutputSchema, DwhConsumiSchema}
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.junit.Assert

class ConsumptionControllerTest extends EnvironmentSparkTest {
  def testComputeMonthlyConsumptions(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val columnsForConsumption = List(
      DwhConsumiSchema.e1, DwhConsumiSchema.e2, DwhConsumiSchema.e3, DwhConsumiSchema.e4, DwhConsumiSchema.e5,
      DwhConsumiSchema.e6, DwhConsumiSchema.e7, DwhConsumiSchema.e8, DwhConsumiSchema.e9, DwhConsumiSchema.e10,
      DwhConsumiSchema.e11, DwhConsumiSchema.e12, DwhConsumiSchema.e13, DwhConsumiSchema.e14, DwhConsumiSchema.e15,
      DwhConsumiSchema.e16, DwhConsumiSchema.e17, DwhConsumiSchema.e18, DwhConsumiSchema.e19, DwhConsumiSchema.e20,
      DwhConsumiSchema.e21, DwhConsumiSchema.e22, DwhConsumiSchema.e23, DwhConsumiSchema.e24, DwhConsumiSchema.e25,
      DwhConsumiSchema.e26, DwhConsumiSchema.e27, DwhConsumiSchema.e28, DwhConsumiSchema.e29, DwhConsumiSchema.e30,
      DwhConsumiSchema.e31, DwhConsumiSchema.e32, DwhConsumiSchema.e33, DwhConsumiSchema.e34, DwhConsumiSchema.e35,
      DwhConsumiSchema.e36, DwhConsumiSchema.e37, DwhConsumiSchema.e38, DwhConsumiSchema.e39, DwhConsumiSchema.e40,
      DwhConsumiSchema.e41, DwhConsumiSchema.e42, DwhConsumiSchema.e43, DwhConsumiSchema.e44, DwhConsumiSchema.e45,
      DwhConsumiSchema.e46, DwhConsumiSchema.e47, DwhConsumiSchema.e48, DwhConsumiSchema.e49, DwhConsumiSchema.e50,
      DwhConsumiSchema.e51, DwhConsumiSchema.e52, DwhConsumiSchema.e53, DwhConsumiSchema.e54, DwhConsumiSchema.e55,
      DwhConsumiSchema.e56, DwhConsumiSchema.e57, DwhConsumiSchema.e58, DwhConsumiSchema.e59, DwhConsumiSchema.e60,
      DwhConsumiSchema.e61, DwhConsumiSchema.e62, DwhConsumiSchema.e63, DwhConsumiSchema.e64, DwhConsumiSchema.e65,
      DwhConsumiSchema.e66, DwhConsumiSchema.e67, DwhConsumiSchema.e68, DwhConsumiSchema.e69, DwhConsumiSchema.e70,
      DwhConsumiSchema.e71, DwhConsumiSchema.e72, DwhConsumiSchema.e73, DwhConsumiSchema.e74, DwhConsumiSchema.e75,
      DwhConsumiSchema.e76, DwhConsumiSchema.e77, DwhConsumiSchema.e78, DwhConsumiSchema.e79, DwhConsumiSchema.e80,
      DwhConsumiSchema.e81, DwhConsumiSchema.e82, DwhConsumiSchema.e83, DwhConsumiSchema.e84, DwhConsumiSchema.e85,
      DwhConsumiSchema.e86, DwhConsumiSchema.e87, DwhConsumiSchema.e88, DwhConsumiSchema.e89, DwhConsumiSchema.e90
    )

    val zeroConsumptionColumns = List(
      DwhConsumiSchema.e91, DwhConsumiSchema.e92, DwhConsumiSchema.e93, DwhConsumiSchema.e94, DwhConsumiSchema.e95,
      DwhConsumiSchema.e96, DwhConsumiSchema.e97, DwhConsumiSchema.e98, DwhConsumiSchema.e99, DwhConsumiSchema.e100
    )

    var dwhConsumi = Environment.sparkContext.parallelize(Seq(
      ("pod1", "M", 1.0001, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 5, "05/2023", 1),
      ("pod1", "-", 1.0001, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2022, 10, "05/2023", 1),
      ("pod1", "-", 1.0001, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2022, 10, "05/2023", 2),

      ("pod2", "F", 1.0015, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 5, "05/2023", 1),
      ("pod2", "-", 1.0015, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 12, "05/2023", 1),
      ("pod2", "-", 1.0015, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 12, "05/2023", 2),

      ("pod3", "C", 1.0155, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 5, "05/2023", 3),
      ("pod3", "-", 1.0155, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 5, "05/2023", 2),
      ("pod3", "-", 1.0155, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 5, "05/2023", 1),

      ("pod4", "O", 1.1555, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 5, "05/2023", 1),
      ("pod5", "X", 1.5555, 1.0005, 1.0050, 1.0500, 1.5000, -1.0, 1.0, 2023, 5, "05/2023", 1),
      ("pod6", "M", -1.0001, -1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2023, 5, "05/2023", 1),
      ("pod7", "F", -1.0001, 1.0, -1.0, 1.0, 1.0, 1.0, -1.0, 2023, 5, "05/2023", 1),
      ("pod8", "C", -1.0001, 1.0, 1.0, -1.0, 1.0, -1.0, 1.0, 2023, 5, "05/2023", 1),
      ("pod9", "O", -1.0001, 1.0, 1.0, 1.0, -1.0, 1.0, 1.0, 2023, 5, "05/2023", 1),

      ("pod10", "F", -1.0001, 1.0, 1.0, 1.0, -1.0, 1.0, 1.0, 2023, 6, "06/2023", 1),

      ("pod3", "O", -1.0001, 1.0, 1.0, 1.0, -1.0, 1.0, 1.0, 2023, 7, "07/2023", 1),
      ("pod3", "O", -1.0001, 1.0, 1.0, 1.0, -1.0, 1.0, 1.0, 2023, 7, "07/2023", 2)
    )).toDF(
      DwhConsumiSchema.pod14,
      DwhConsumiSchema.trattamento,
      DwhConsumiSchema.somma_eam,
      DwhConsumiSchema.somma_eaf1,
      DwhConsumiSchema.somma_eaf2,
      DwhConsumiSchema.somma_eaf3,
      DwhConsumiSchema.somma_eaf4,
      DwhConsumiSchema.somma_eaf5,
      DwhConsumiSchema.somma_eaf6,
      DwhConsumiSchema.anno,
      DwhConsumiSchema.mese,
      DwhConsumiOutputSchema.meseanno,
      DwhConsumiSchema.versione
    )

    columnsForConsumption.foreach(colName =>
      dwhConsumi = dwhConsumi.withColumn(colName, lit(1.0))
    )

    zeroConsumptionColumns.foreach(colName =>
      dwhConsumi = dwhConsumi.withColumn(colName, lit(null).cast(DoubleType))
    )

    val output = ConsumptionController.computeMonthlyConsumptions(dwhConsumi)

    output.sort(col(DwhConsumiOutputSchema.codice_pod)).cache.show

    Assert.assertEquals(11, output.count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod1" && col(DwhConsumiOutputSchema.consumo) === 1.0).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod2" && col(DwhConsumiOutputSchema.consumo) === 5.556).count)
    Assert.assertEquals(2, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod3").count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod3" && col(DwhConsumiOutputSchema.consumo) === 5.556).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod4" && col(DwhConsumiOutputSchema.consumo) === 90.0).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod5" && col(DwhConsumiOutputSchema.consumo).isNull).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod6" && col(DwhConsumiOutputSchema.consumo) === 0.0).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod7" && col(DwhConsumiOutputSchema.consumo) === 4.0).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod8" && col(DwhConsumiOutputSchema.consumo) === 4.0).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod9" && col(DwhConsumiOutputSchema.consumo) === 90.0).count)
    Assert.assertEquals(1, output.where(col(DwhConsumiOutputSchema.codice_pod) === "pod10" && col(DwhConsumiOutputSchema.consumo) === 5.0).count)
  }
}
