package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class AggregatoTotaleTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val aggregatoTotaleDAO = new AggregatoTotaleDAOMock()
    val result = aggregatoTotaleDAO.get
    result.collect.foreach(println)

    Assert.assertEquals(4, result.count)
  }

  class AggregatoTotaleDAOMock extends AggregatoTotaleDAO {
    override def readTable: DataFrame = {
      val sqlContext = Environment.sqlContext
      import sqlContext.implicits._
      Environment.sparkContext.parallelize(Seq(
        (1L, "distr1", "azienda1", "udd1", "azienda8"),
        (2L, "distr2", "azienda2", "udd2", "azienda9"),
        (3L, "distr3", "azienda3", "udd3", "azienda10"),
        (4L, "distr3", "azienda3", "udd3", "azienda10")
      ))
        .toDF(
          AggregatoTotaleSchema.id_indennizzo,
          AggregatoTotaleSchema.piva_distr,
          AggregatoTotaleSchema.rag_soc_distr,
          AggregatoTotaleSchema.piva_udd,
          AggregatoTotaleSchema.rag_soc_udd)
        .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om2, lit(100.0))
        .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om2, lit(100.0))
        .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om3, lit(30.0))
        .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om3, lit(100.0))
        .withColumn(AggregatoTotaleSchema.pdr_g, lit(102))
        .withColumn(AggregatoTotaleSchema.pdr_g_om1, lit(100))
        .withColumn(AggregatoTotaleSchema.pdr_g_om2, lit(75))
        .withColumn(AggregatoTotaleSchema.pdr_g_om3, lit(15))
        .withColumn(AggregatoTotaleSchema.target_percentage_om1, lit(98.0))
        .withColumn(AggregatoTotaleSchema.target_percentage_om2, lit(75.0))
        .withColumn(AggregatoTotaleSchema.target_percentage_om3, lit(15.0))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om1, lit(100.0))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om2, lit(100.0))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om3, lit(100.0))
        .withColumn(AggregatoTotaleSchema.pdr_target_om1, lit(100.0))
        .withColumn(AggregatoTotaleSchema.pdr_target_om2, lit(75.0))
        .withColumn(AggregatoTotaleSchema.pdr_target_om3, lit(15.0))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om1, lit(0.0))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om2, lit(0.0))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om3, lit(0.0))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om1, lit(35.0))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om2, lit(36.0))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om3, lit(16.0))
        .withColumn(AggregatoTotaleSchema.indennizzo_om1, lit(0.0))
        .withColumn(AggregatoTotaleSchema.indennizzo_om2, lit(0.0))
        .withColumn(AggregatoTotaleSchema.indennizzo_om3, lit(0.0))
        .withColumn(AggregatoTotaleSchema.annomese, lit("2022"))
        .withColumn(AggregatoTotaleSchema.executionid, lit(12345L))
    }
  }
}
