package it.eng.au.aggiustamentoGas.dao.rcugas

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasTech
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasVarConvertitorePSchema, RcuGasVarMisuratorePSchema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import junit.framework.TestCase
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class RcuGasMisuratorePDAOTest extends TestCase with EnvironmentSparkTest {
/*
  def testRead(): Unit = {


    val emptyDF = Environment.getSpark.createDataFrame(Seq.empty[RcuGasTech])
      .withColumnRenamed(RcuGasVarConvertitorePSchema.d_data_inizio, "d_inizio_conv")
      .withColumnRenamed(RcuGasVarConvertitorePSchema.d_data_fine, "d_fine_conv")

    val rcuList = (new RcuGasVarMisuratorePDAOMock).get("202111", "202201", emptyDF).cache()

    rcuList.collect.foreach(println)
    Assert.assertEquals(2, rcuList.count())

  }

  class RcuGasVarMisuratorePDAOMock extends RcuGasVarMisuratorePDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("1", "", "", "", "", "", null, null)
        , ("1", "", "", "", "", "", "2022-01-01 00:00:00.0", "2022-01-01 00:00:00.0")
      ).toDF(
        RcuGasVarMisuratorePSchema.n_id_pdr,
        RcuGasVarMisuratorePSchema.t_misuratore_integrato,
        RcuGasVarMisuratorePSchema.t_classe_misuratore,
        RcuGasVarMisuratorePSchema.t_presenza_convertitore,
        RcuGasVarMisuratorePSchema.n_coeff_correzione,
        RcuGasVarMisuratorePSchema.n_num_cifre_misuratore,
        RcuGasVarMisuratorePSchema.d_data_inizio,
        RcuGasVarMisuratorePSchema.d_data_fine
      )
    }
  }*/

}
