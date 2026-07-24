package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.schema.cig.{PdrCountSchema, PdrTotaleSchema}
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions.{col, hash}
import org.apache.spark.sql.types.LongType
import org.junit.Assert

class IndennizziControllerTest extends EnvironmentSparkTest {
  def testCalcoloTest(): Unit = {
    Environment.setProperty("om1.target.percent", "98.0")
    Environment.setProperty("om2.target.percent", "75.0")
    Environment.setProperty("om3.target.percent", "15.0")
    Environment.setProperty("om1.euro.fee", "35.0")
    Environment.setProperty("om2.euro.fee", "36.0")
    Environment.setProperty("om3.euro.fee", "12.0")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrCountTest = Environment.sparkContext.parallelize(Seq(
      ("udd1", "azienda1", "distr1", "azienda6", 100, 0, 0, 0),
      ("udd2", "azienda2", "distr2", "azienda7", 100, 100, 0, 0),
      ("udd3", "azienda3", "distr3", "azienda8", 100, 100, 100, 0),
      ("udd4", "azienda4", "distr4", "azienda9", 100, 100, 0, 100)
    )).toDF(PdrCountSchema.getValues: _*)

    val resultTest = IndennizziController.calcoloIndennizzi(pdrCountTest)
    resultTest.show
    resultTest.printSchema

    Assert.assertEquals(4, resultTest.count())
    Assert.assertEquals(33, resultTest.columns.length)
    Assert.assertEquals(3, resultTest.where(col(AggregatoTotaleSchema.achieved_percentage_om1) >= 98.0).count())
    Assert.assertEquals(2, resultTest.where(col(AggregatoTotaleSchema.achieved_percentage_om2) >= 75.0).count())
    Assert.assertEquals(3, resultTest.where(col(AggregatoTotaleSchema.achieved_percentage_om3) >= 15.0).count())
    Assert.assertEquals(1, resultTest.where(col(AggregatoTotaleSchema.delta_pdr_om1) < 0).count())
    Assert.assertEquals(2, resultTest.where(col(AggregatoTotaleSchema.delta_pdr_om2) < 0).count())
    Assert.assertEquals(1, resultTest.where(col(AggregatoTotaleSchema.delta_pdr_om3) < 0).count())
    Assert.assertEquals(1, resultTest.where(col(AggregatoTotaleSchema.indennizzo_om1) > 0).count())
    Assert.assertEquals(2, resultTest.where(col(AggregatoTotaleSchema.indennizzo_om2) > 0).count())
    Assert.assertEquals(1, resultTest.where(col(AggregatoTotaleSchema.indennizzo_om3) > 0).count())
  }

  def testCalcoloExample(): Unit = {
    Environment.setProperty("om1.target.percent", "98.0")
    Environment.setProperty("om2.target.percent", "75.0")
    Environment.setProperty("om3.target.percent", "15.0")
    Environment.setProperty("om1.euro.fee", "35.0")
    Environment.setProperty("om2.euro.fee", "36.0")
    Environment.setProperty("om3.euro.fee", "12.0")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrCountExample = Environment.sparkContext.parallelize(Seq(
      ("udd1", "azienda1", "distr1", "azienda1", 27114, 27055, 10133, 11705)
    )).toDF(PdrCountSchema.getValues: _*)

    val resultExample = IndennizziController.calcoloIndennizzi(pdrCountExample)
    resultExample.show

    Assert.assertEquals(1, resultExample.count())
    Assert.assertEquals(33, resultExample.columns.length)
    Assert.assertEquals(1, resultExample.where(col(AggregatoTotaleSchema.achieved_percentage_om1) >= 98.0).count())
    Assert.assertEquals(0, resultExample.where(col(AggregatoTotaleSchema.achieved_percentage_om2) >= 75.0).count())
    Assert.assertEquals(1, resultExample.where(col(AggregatoTotaleSchema.achieved_percentage_om3) >= 15.0).count())
    Assert.assertEquals(0, resultExample.where(col(AggregatoTotaleSchema.delta_pdr_om1) < 0).count())
    Assert.assertEquals(1, resultExample.where(col(AggregatoTotaleSchema.delta_pdr_om2) < 0).count())
    Assert.assertEquals(0, resultExample.where(col(AggregatoTotaleSchema.delta_pdr_om3) < 0).count())
    Assert.assertEquals(0, resultExample.where(col(AggregatoTotaleSchema.indennizzo_om1) > 0).count())
    Assert.assertEquals(1, resultExample.where(col(AggregatoTotaleSchema.indennizzo_om2) > 0).count())
    Assert.assertEquals(0, resultExample.where(col(AggregatoTotaleSchema.indennizzo_om3) > 0).count())
  }

  def testCalcoloOM1(): Unit = {
    Environment.setProperty("om1.target.percent", "98.0")
    Environment.setProperty("om2.target.percent", "75.0")
    Environment.setProperty("om3.target.percent", "15.0")
    Environment.setProperty("om1.euro.fee", "35.0")
    Environment.setProperty("om2.euro.fee", "36.0")
    Environment.setProperty("om3.euro.fee", "12.0")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrCountOM1 = Environment.sparkContext.parallelize(Seq(
      ("udd1", "azienda1", "distr1", "azienda6", 100, 100, 100, 0),
      ("udd2", "azienda2", "distr2", "azienda7", 100, 98, 98, 0),
      ("udd3", "azienda3", "distr3", "azienda8", 100, 50, 50, 0)
    )).toDF(PdrCountSchema.getValues: _*)

    val resultOM1 = IndennizziController.calcoloIndennizzi(pdrCountOM1)
    resultOM1.show

    Assert.assertEquals(3, resultOM1.count())
    Assert.assertEquals(33, resultOM1.columns.length)
    Assert.assertEquals(2, resultOM1.where(col(AggregatoTotaleSchema.achieved_percentage_om1) >= 98.0).count())
    Assert.assertEquals(3, resultOM1.where(col(AggregatoTotaleSchema.pdr_target_om1) === 98.0).count())
    Assert.assertEquals(1, resultOM1.where(col(AggregatoTotaleSchema.delta_pdr_om1) < 0).count())
    Assert.assertEquals(1, resultOM1.where(col(AggregatoTotaleSchema.indennizzo_om1) > 0).count())
  }

  def testCalcoloOM2(): Unit = {
    Environment.setProperty("om1.target.percent", "98.0")
    Environment.setProperty("om2.target.percent", "75.0")
    Environment.setProperty("om3.target.percent", "15.0")
    Environment.setProperty("om1.euro.fee", "35.0")
    Environment.setProperty("om2.euro.fee", "36.0")
    Environment.setProperty("om3.euro.fee", "12.0")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrCountOM2 = Environment.sparkContext.parallelize(Seq(
      ("udd1", "azienda1", "distr1", "azienda6", 100, 100, 90, 10),
      ("udd2", "azienda2", "distr2", "azienda7", 100, 100, 75, 25),
      ("udd3", "azienda3", "distr3", "azienda8", 100, 100, 50, 50)
    )).toDF(PdrCountSchema.getValues: _*)

    val resultOM2 = IndennizziController.calcoloIndennizzi(pdrCountOM2)
    resultOM2.show

    Assert.assertEquals(3, resultOM2.count())
    Assert.assertEquals(33, resultOM2.columns.length)
    Assert.assertEquals(2, resultOM2.where(col(AggregatoTotaleSchema.achieved_percentage_om2) >= 75.0).count())
    Assert.assertEquals(3, resultOM2.where(col(AggregatoTotaleSchema.pdr_target_om2) === 75.0).count())
    Assert.assertEquals(1, resultOM2.where(col(AggregatoTotaleSchema.delta_pdr_om2) < 0).count())
    Assert.assertEquals(1, resultOM2.where(col(AggregatoTotaleSchema.indennizzo_om2) > 0).count())
  }

  def testCalcoloOM3(): Unit = {
    Environment.setProperty("om1.target.percent", "98.0")
    Environment.setProperty("om2.target.percent", "75.0")
    Environment.setProperty("om3.target.percent", "15.0")
    Environment.setProperty("om1.euro.fee", "35.0")
    Environment.setProperty("om2.euro.fee", "36.0")
    Environment.setProperty("om3.euro.fee", "12.0")

    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val pdrCountOM3 = Environment.sparkContext.parallelize(Seq(
      ("udd1", "azienda1", "distr1", "azienda6", 100, 100, 75, 0),
      ("udd2", "azienda2", "distr2", "azienda7", 100, 100, 0, 15),
      ("udd3", "azienda3", "distr3", "azienda8", 100, 100, 0, 10),
      ("udd4", "azienda4", "distr4", "azienda9", 100, 100, 80, 9),
      ("udd5", "azienda5", "distr5", "azienda10", 100, 100, 80, 10),
      ("udd6", "azienda6", "distr6", "azienda11", 100, 100, 90, 10)
    )).toDF(PdrCountSchema.getValues: _*)

    val resultOM3 = IndennizziController.calcoloIndennizzi(pdrCountOM3)
    resultOM3.show

    Assert.assertEquals(6, resultOM3.count())
    Assert.assertEquals(33, resultOM3.columns.length)
    Assert.assertEquals(3, resultOM3.where(col(AggregatoTotaleSchema.achieved_percentage_om3) >= 15.0).count())
    Assert.assertEquals(6, resultOM3.where(col(AggregatoTotaleSchema.pdr_target_om3) === 15.0).count())
    Assert.assertEquals(3, resultOM3.where(col(AggregatoTotaleSchema.delta_pdr_om3) < 0).count())
    Assert.assertEquals(3, resultOM3.where(col(AggregatoTotaleSchema.indennizzo_om3) > 0).count())
  }

  def testHash(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val df = Environment.sparkContext.parallelize(Seq(
      ("udd1", "distr1", "12345"),
      ("udd1", "distr1", "12345"),
      ("udd2", "distr2", "12345"),
      ("udd2", "distr2", "12350")
    )).toDF(AggregatoTotaleSchema.piva_udd, AggregatoTotaleSchema.piva_distr, AggregatoTotaleSchema.executionid)
      .withColumn(AggregatoTotaleSchema.id_indennizzo, hash(col(PdrTotaleSchema.piva_udd), col(PdrTotaleSchema.piva_distr), col(PdrTotaleSchema.executionid)).cast(LongType) + Int.MaxValue)

    df.show
    df.printSchema
  }
}
