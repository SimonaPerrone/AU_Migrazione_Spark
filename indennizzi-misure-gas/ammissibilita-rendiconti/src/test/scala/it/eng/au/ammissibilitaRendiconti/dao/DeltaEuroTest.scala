package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.EnvironmentSparkTest
import it.eng.au.ammissibilitaRendiconti.schema.DeltaEuroSchema
import it.eng.au.indennizziMisureGasCommon.schema.IndennizziRzg2Schema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.functions.col
import org.junit.Assert

class DeltaEuroTest extends EnvironmentSparkTest {
  def testGetDeltaEuro(): Unit = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val deltaEuroDAOMock = new DeltaEuroDAOMock

    val indennizziRzg2 = Environment.sparkContext.parallelize(Seq(
      (1L, "distr1", "azienda1", "udd1", "azienda1", Some(0.0),   Some(5.0),  Some(10.0), Some(0.0), Some(5.0), Some(10.0), Some(0.0),  Some(0.0),  Some(0.0),  "202211", 123456L),
      (2L, "distr2", "azienda2", "udd2", "azienda2", Some(0.0),   Some(5.0),  Some(10.0), Some(0.0), Some(5.0), Some(10.0), Some(0.0),  Some(0.0),  Some(0.0),  "202211", 123456L),
      (3L, "distr3", "azienda3", "udd3", "azienda3", Some(0.0),   Some(5.0),  None,       Some(0.0), Some(5.0), Some(10.0), Some(0.0),  Some(0.0),  None,       "202211", 123456L),
      (4L, "distr4", "azienda4", "udd4", "azienda4", Some(0.0),   None,       Some(10.0), Some(0.0), Some(5.0), Some(10.0), Some(0.0),  None,       Some(0.0),  "202211", 123456L),
      (5L, "distr5", "azienda5", "udd5", "azienda5", None,        Some(5.0),  Some(10.0), Some(0.0), Some(5.0), Some(10.0), None,       Some(0.0),  Some(0.0),  "202211", 123456L),
      (6L, "distr6", "azienda6", "udd6", "azienda6", Some(10.0),  Some(15.0), Some(20.0), Some(0.0), Some(5.0), Some(10.0), Some(10.0), Some(10.0), Some(10.0), "202211", 123456L),
      (7L, "distr7", "azienda7", "udd7", "azienda7", None,        None,       None,       None,      None,      None,       None,       None,       None,       "202211", 123456L)
    )).toDF(
      IndennizziRzg2Schema.csv_id_indennizzo,
      IndennizziRzg2Schema.piva_id,
      IndennizziRzg2Schema.csv_rag_soc_id,
      IndennizziRzg2Schema.piva_udd,
      IndennizziRzg2Schema.csv_rag_soc_udd,
      IndennizziRzg2Schema.euro_sii_om1,
      IndennizziRzg2Schema.euro_sii_om2,
      IndennizziRzg2Schema.euro_sii_om3,
      IndennizziRzg2Schema.csv_euro_om1,
      IndennizziRzg2Schema.csv_euro_om2,
      IndennizziRzg2Schema.csv_euro_om3,
      IndennizziRzg2Schema.delta_om1,
      IndennizziRzg2Schema.delta_om2,
      IndennizziRzg2Schema.delta_om3,
      IndennizziRzg2Schema.anno_mese_competenza,
      IndennizziRzg2Schema.executionid
    )

    val result = deltaEuroDAOMock.get(indennizziRzg2)
    result.orderBy(col(DeltaEuroSchema.piva_udd)).show

    Assert.assertEquals(7, result.count)
    Assert.assertEquals(16, result.columns.length)
    Assert.assertEquals(2, result.where(col(DeltaEuroSchema.delta_om1).isNull).count)
    Assert.assertEquals(0, result.where(col(DeltaEuroSchema.delta_om3) < 0.0).count)
  }

  class DeltaEuroDAOMock extends DeltaEuroDAO {
    override val parquetPath = "src/test/resources/output/delta_euro"
  }
}
