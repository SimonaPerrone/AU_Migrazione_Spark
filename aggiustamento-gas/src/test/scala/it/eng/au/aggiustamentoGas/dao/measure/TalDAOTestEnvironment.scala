package it.eng.au.aggiustamentoGas.dao.measure

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure.Tal
import it.eng.au.aggiustamentoGas.schema.measure.TalSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.junit.Assert

class TalDAOTestEnvironment extends EnvironmentSparkTest {
  def testGet(): Unit = {
    val result = TalDAOMock.get("202101", "202101", getTreatment = false).cache()

    Assert.assertEquals(6, result.count)
    //Check Flow.date is properly valued
    Assert.assertEquals("30/11/2021", result.filter(_.pdr == "1").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("31/05/2019", result.filter(_.pdr == "2").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("28/05/2019", result.filter(_.pdr == "3").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("03/05/2019", result.filter(_.pdr == "4").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertEquals("03/05/2019", result.filter(_.pdr == "5").first().date.get.toString("dd/MM/yyyy"))
    Assert.assertTrue(result.filter(_.pdr == "6").first().date.isEmpty)
    //Check Tal.outcome is properly valued
    Assert.assertEquals('v', result.filter(_.pdr == "1").first().asInstanceOf[Tal].outcome.get)
    Assert.assertEquals('V', result.filter(_.pdr == "2").first().asInstanceOf[Tal].outcome.get)
    Assert.assertEquals('V', result.filter(_.pdr == "3").first().asInstanceOf[Tal].outcome.get)
    Assert.assertEquals('v', result.filter(_.pdr == "4").first().asInstanceOf[Tal].outcome.get)
    Assert.assertEquals('V', result.filter(_.pdr == "5").first().asInstanceOf[Tal].outcome.get)
    Assert.assertEquals('v', result.filter(_.pdr == "6").first().asInstanceOf[Tal].outcome.get)
    Assert.assertEquals(0, result.filter(_.pdr == "8").count)
  }

  object TalDAOMock extends TalDAO {
    override def readParquet: DataFrame = {
      val sqlCtx = Environment.getSpark.sqlContext
      import sqlCtx.implicits._

      List(
        ("TAL", "1", "30/11/2021", "null", "000010343", "null", "27545059", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_01396650218/DISTRIBUTORE/TMG_01396650218_12883420155/2020/0115/01396650218_12883420155_201911_TAL0150_20191204031957_1.xml", Option(""), "",
          "", "2020-01-16T03:20:25.618901", "v", "202101"),
        ("TAL", "2", "31/05/2019", "null", "000003233", "null", "0084966131", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00757920152/DISTRIBUTORE/TMG_00757920152_07451521004/2020/0316/00757920152_07451521004_202003_TAL0150_20200316203028_1.xml", Option(""), "",
          "", "2020-04-08T15:31:01.687440", "V", "202101"),
        ("TAL", "3", "28/05/2019", "null", "000004300", "null", "ITGF030001789664", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12300020158/2020/0610/00489490011_12300020158_202006_TAL0150_20200610203008_1.xml", Option(""), "",
          "", "2020-06-11T04:16:40.919252", "V", "202101"),
        ("TAL", "4", "NULL", "03/05/2019", "000010080", "null", "270953580028015284", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03479071205/DISTRIBUTORE/TMG_03479071205_07451521004/2020/0316/03479071205_07451521004_201905_TAL.0150_20200316180005_0001.xml", Option(""), "",
          "", "2020-04-08T15:31:01.687440", "v", "202101"),
        ("TAL", "5", "null", "03/05/2019", "000020862", "null", "251050000027644326", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03479071205/DISTRIBUTORE/TMG_03479071205_07451521004/2020/0316/03479071205_07451521004_201905_TAL.0150_20200316180005_0001.xml", Option(""), "",
          "", "2020-04-08T15:31:01.687440", "V", "202101"),
        ("TAL", "6", "null", "NULL", "000000656", "null", "229170032110879073", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03479071205/DISTRIBUTORE/TMG_03479071205_07451521004/2020/0316/03479071205_07451521004_201905_TAL.0150_20200316180005_0001.xml", Option(""), "",
          "", "2020-04-08T15:31:01.687440", "v", "202101"),
        ("TAL", "7", "null", "NULL", "000000656", "null", "229170032110879073", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03479071205/DISTRIBUTORE/TMG_03479071205_07451521004/2020/0316/03479071205_07451521004_201905_TAL.0150_20200316180005_0001.xml", Option(""), "",
          "", "2020-04-08T15:31:01.687440", "F", "202101"),
        ("TAL", "8", "null", "NULL", "000000656", "null", "229170032110879073", "null",
          "/mnt/isilonshare1/GAS_INJ/TMG_03479071205/DISTRIBUTORE/TMG_03479071205_07451521004/2020/0316/03479071205_07451521004_201905_TAL.0150_20200316180005_0001.xml", None, "",
          "", "2020-04-08T15:31:01.687440", "F", "202101")

      ).toDF(
        TalSchema.cod_servizio,
        TalSchema.cod_pdr,
        TalSchema.data_com_autolet_cf,
        TalSchema.data_racc,
        TalSchema.let_tot_prel,
        TalSchema.let_tot_conv,
        TalSchema.matr_mis,
        TalSchema.matr_conv,
        TalSchema.local_file,
        TalSchema.ammissibilita,
        TalSchema.piva_distr,
        TalSchema.piva_utente,
        TalSchema.d_caricamento,
        TalSchema.esito_val,
        MeasureDAO.ANNO_MESE_COL_NAME
      )
    }
  }

}
