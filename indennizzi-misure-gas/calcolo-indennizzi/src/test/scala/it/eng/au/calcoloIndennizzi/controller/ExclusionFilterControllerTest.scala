package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.EnvironmentSparkTest
import it.eng.au.calcoloIndennizzi.model.measure.Tgl
import it.eng.au.calcoloIndennizzi.schema.measure.TglSchema
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasMassivoPSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime
import org.junit.Assert

class ExclusionFilterControllerTest extends EnvironmentSparkTest {
  def testExcludePdrs(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val rcugasMassivo: DataFrame = Environment.sparkContext.parallelize(Seq(
      ("nIdPdr1", "pdr1", "udd1"),
      ("nIdPdr2", "pdr2", "udd2"),
      ("nIdPdr3", "pdr3", "udd3"),
      ("nIdPdr4", "pdr4", "udd4"),
      ("nIdPdr5", "pdr5", "udd5"),
      ("nIdPdr6", "pdr6", "udd6")
    )).toDF(RcugasMassivoPSchema.n_id_pdr, RcugasMassivoPSchema.t_codice_pdr, RcugasMassivoPSchema.piva_udd)

    val result = ExclusionFilterController.excludePdrs(rcugasMassivo)
    result.show

    Assert.assertEquals(3, result.count)
    Assert.assertEquals(3, result.columns.length)
    Assert.assertEquals(0, result.where(col(RcugasMassivoPSchema.t_codice_pdr) === "pdr3").count)
    Assert.assertEquals(1, result.where(col(RcugasMassivoPSchema.t_codice_pdr) === "pdr2").count)
  }

  def testExcludeTgls(): Unit = {
    val measures = Environment.sparkContext.parallelize(Seq(
      Tgl("pdr1", Some(DateTime.parse("2022-10-01")), None, None, None, None, None, None, Some("/mnt/localfile1"), None, None, None, None),
      Tgl("pdr2", Some(DateTime.parse("2022-10-01")), None, None, None, None, None, None, Some("/mnt/localfile2"), None, None, None, None),
      Tgl("pdr3", Some(DateTime.parse("2022-10-01")), None, None, None, None, None, None, Some("/mnt/localfile3"), None, None, None, None),
      Tgl("pdr4", Some(DateTime.parse("2022-10-01")), None, None, None, None, None, None, Some("/mnt/localfile4"), None, None, None, None),
      Tgl("pdr5", Some(DateTime.parse("2022-10-01")), None, None, None, None, None, None, Some("/mnt/localfile5"), None, None, None, None),
      Tgl("pdr5", Some(DateTime.parse("2022-10-01")), None, None, None, None, None, None, Some("/mnt/localfile6"), None, None, None, None),
      Tgl("pdr6", Some(DateTime.parse("2022-10-01")), None, None, None, None, None, None, Some("/mnt/localfile5"), None, None, None, None)
    ))

    val result = ExclusionFilterController.excludeTgls(measures)
    result.collect.foreach(println)

    Assert.assertEquals(4, result.count)
    Assert.assertEquals(0, result.filter(_.pdr == "pdr1").count)
    Assert.assertEquals(1, result.filter(_.pdr == "pdr5").count)
    Assert.assertEquals(1, result.filter(_.pdr == "pdr6").count)
  }
}
