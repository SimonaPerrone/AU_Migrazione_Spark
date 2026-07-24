package it.eng.au.calcoloIndennizzi.dao.cig

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.dao.output.{DettaglioOM1DAO, DettaglioOM2DAO, DettaglioOM3DAO}
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class DettaglioOMTest extends EnvironmentSparkTest {
  val sqlContext = Environment.sqlContext

  import sqlContext.implicits._

  val aggregatoTotale = Environment.sparkContext.parallelize(Seq(
    (1L, "distr1", "azienda1", "udd1", "azienda7"),
    (2L, "distr2", "azienda2", "udd2", "azienda8"),
    (3L, "distr3", "azienda3", "udd3", "azienda9"),
    (4L, "distr4", "azienda4", "udd4", "azienda10"),
    (5L, "distr5", "azienda5", "udd5", "azienda11"),
    (6L, "distr6", "azienda6", "udd6", "azienda12")
  )).toDF(
    AggregatoTotaleSchema.id_indennizzo,
    AggregatoTotaleSchema.piva_distr,
    AggregatoTotaleSchema.rag_soc_distr,
    AggregatoTotaleSchema.piva_udd,
    AggregatoTotaleSchema.rag_soc_udd)
    .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om2, lit(100.0))
    .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om2, lit(100.0))
    .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om3, lit(30.0))
    .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om3, lit(100.0))
    .withColumn(AggregatoTotaleSchema.pdr_g, lit(101))
    .withColumn(AggregatoTotaleSchema.pdr_g_om1, lit(100))
    .withColumn(AggregatoTotaleSchema.pdr_g_om2, lit(75))
    .withColumn(AggregatoTotaleSchema.pdr_g_om3, lit(25))
    .withColumn(AggregatoTotaleSchema.target_percentage_om1, lit(98.0))
    .withColumn(AggregatoTotaleSchema.target_percentage_om2, lit(75.0))
    .withColumn(AggregatoTotaleSchema.target_percentage_om3, lit(15.0))
    .withColumn(AggregatoTotaleSchema.achieved_percentage_om1, lit(99.0))
    .withColumn(AggregatoTotaleSchema.achieved_percentage_om2, lit(75.0))
    .withColumn(AggregatoTotaleSchema.achieved_percentage_om3, lit(25.0))
    .withColumn(AggregatoTotaleSchema.pdr_target_om1, lit(98.98))
    .withColumn(AggregatoTotaleSchema.pdr_target_om2, lit(75.0))
    .withColumn(AggregatoTotaleSchema.pdr_target_om3, lit(15.0))
    .withColumn(AggregatoTotaleSchema.delta_pdr_om1, lit(1.02))
    .withColumn(AggregatoTotaleSchema.delta_pdr_om2, lit(0.0))
    .withColumn(AggregatoTotaleSchema.delta_pdr_om3, lit(10.0))
    .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om1, lit(35.0))
    .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om2, lit(36.0))
    .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om3, lit(16.0))
    .withColumn(AggregatoTotaleSchema.indennizzo_om1, lit(0.0))
    .withColumn(AggregatoTotaleSchema.indennizzo_om2, lit(0.0))
    .withColumn(AggregatoTotaleSchema.indennizzo_om3, lit(0.0))
    .withColumn(AggregatoTotaleSchema.annomese, lit("202211"))
    .withColumn(AggregatoTotaleSchema.executionid, lit(12345L))


  def testGetDettaglioOM1(): Unit = {
    val dettaglioOM1DAO = new DettaglioOM1DAO

    val result = dettaglioOM1DAO.get(aggregatoTotale)
    result.show

    Assert.assertEquals(6, result.count)
    Assert.assertEquals(15, result.columns.length)
  }

  def testGetDettaglioOM2(): Unit = {
    val dettaglioOM2DAO = new DettaglioOM2DAO

    val result = dettaglioOM2DAO.get(aggregatoTotale)
    result.show

    Assert.assertEquals(6, result.count)
    Assert.assertEquals(17, result.columns.length)
  }

  def testGetDettaglioOM3(): Unit = {
    val dettaglioOM3DAO = new DettaglioOM3DAO

    val result = dettaglioOM3DAO.get(aggregatoTotale)
    result.show

    Assert.assertEquals(6, result.count)
    Assert.assertEquals(18, result.columns.length)
  }
}
