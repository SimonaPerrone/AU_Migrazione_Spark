package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasMassivoPSchema
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasMassivoPSchema._
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.junit.Assert

class RcuGasMassivoPDAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = new RcuGasMassivoPDAOMock().get().cache

    result.collect.foreach(println)

    Assert.assertEquals(2, result.count)

    Assert.assertEquals(1, result.filter(_.tCodicePdr == "1").count)
    Assert.assertEquals("2016-12-01", result.filter(_.tCodicePdr == "1").first.startDate.toString("yyyy-MM-dd"))
    Assert.assertEquals("2016-12-31", result.filter(_.tCodicePdr == "1").first.endDate.toString("yyyy-MM-dd"))

    Assert.assertEquals(1, result.filter(_.tCodicePdr == "2").count)
    Assert.assertEquals("1970-01-01", result.filter(_.tCodicePdr == "2").first.startDate.toString("yyyy-MM-dd"))
    Assert.assertEquals("2999-12-31", result.filter(_.tCodicePdr == "2").first.endDate.toString("yyyy-MM-dd"))
    Assert.assertEquals(Treatment.N, result.filter(_.tCodicePdr == "2").first.tTrattamento)

  }

  class RcuGasMassivoPDAOMock extends RcuGasMassivoPDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("2016-12-01 00:00:00.0", "2016-12-31 00:00:00.0", "1", "a", "C3", "C3C1", "VARIAZIONE", "Y", "1.02", "SI", "NO", "06289781004","","400", "id_for"),
        (null, null, "2", null, null, null, null, null, null, null, null, null, null, null, "id_for"),
        ("2016-12-01 00:00:00.0", "2016-12-31 00:00:00.0", "1", "a", "C3", "C3C1", "VARIAZIONE", "Y", "1.02", "SI", "NO", "06289781004","","400", null)
      ).toDF(
        d_data_inizio_for,
        data_fine_for,
        t_codice_pdr,
        n_id_pdr,
        t_cod_cat_uso,
        t_cod_profilo,
        t_processo,
        t_trattamento,
        n_coeff_correzione,
        t_misuratore_integrato,
        t_pre_conv,
        piva_udd,
        t_tipo_fornitura,
        n_prelievo_annuo,
        n_id_fornitura
      ).withColumn(RcuGasMassivoPSchema.t_comune_istatforn, lit("Cod1"))
        .withColumn(RcuGasMassivoPSchema.t_comune_istat_pdr, lit("Cod2"))
    }

  }
}
