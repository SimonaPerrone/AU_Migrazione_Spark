package it.eng.au.aggiustamentoGas.filter.inclusion

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.model.measure.{Flow, Rml, Tgl}
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasConnessioniDistr2Schema, RcuGasMassivoPSchema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.junit.Assert

class TestInclusionFiltersEnvironment extends EnvironmentSparkTest {

  def testFilterByPdr(): Unit = {
    Environment.setProperty("filter.inclusion.pdr.enabled", "true")
    val flowRDD: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(
      List(
        Tgl(service = "TGL", pdr = "1", readType = None, date = None, measure = None, converted = None,
          serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        ),
        Tgl(service = "TGL", pdr = "1", readType = None, date = None, measure = None, converted = None,
          serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        ),
        Rml(service = "RML", pdr = "11", date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, motivation = None, dataCaricamento = None, freqLet = None, readType = None, tipoRettifica = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml")
        ),
        Tgl(service = "TGL", pdr = "12", readType = None, date = None, measure = None, converted = None,
          serialNumberMis = None, pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")
        ),
        Rml(service = "RML", pdr = "13", date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, motivation = None, dataCaricamento = None, freqLet = None, readType = None, tipoRettifica = None,
          localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")
        )
      )
    )
    val inclusionFilterController: InclusionFilterController = new InclusionPdrFilter
    inclusionFilterController.inclusionFileDf.show(false)

    val filteredFLows = inclusionFilterController.filter(flowRDD).cache

    filteredFLows.collect.foreach(println)

    Assert.assertTrue(inclusionFilterController.isEnabled)
    Assert.assertEquals(2, filteredFLows.filter(_.pdr.equals("1")).count)
    Assert.assertEquals(0, filteredFLows.filter(!_.pdr.equals("1")).count)

  }

  def testFilterByIdDistr(): Unit = {

    Environment.setProperty("rcugas.sqoop.date", "20210611")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "true")

    val measureRDD: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(
      List(
        Tgl(service = "TGL", pdr = "pdr", readType = None, date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None),
        Tgl(service = "TGL", pdr = "pdrPrime", readType = None, date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None)
        )
    )
    val sqlCtx =  Environment.getSpark.sqlContext
    import sqlCtx.implicits._
    val conn2DistrDF =  Environment.getSpark.sparkContext.parallelize(
      List(
        ("pdr", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "id1"),
        ("pdrPrime", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0", "id1")
      )
    ).toDF(
      RcuGasConnessioniDistr2Schema.t_codice_pdr,
      RcuGasConnessioniDistr2Schema.d_data_inizio_conn,
      RcuGasConnessioniDistr2Schema.d_data_fine_conn,
      RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione,
      RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione,
      RcuGasConnessioniDistr2Schema.n_id_distr
    )

    val iFC: InclusionFilterController = new InclusionIdDistrFilter(conn2DistrDF)
    iFC.inclusionFileDf.show(false)
    val filteredMeasures: RDD[Flow] = iFC.filter(measureRDD)

    Assert.assertTrue(iFC.isEnabled)
    Assert.assertEquals(1, filteredMeasures.filter(_.pdr.equals("pdr")).count)
    Assert.assertEquals(0, filteredMeasures.filter(_.pdr.equals("pdrPrime")).count)

  }

  def testFilterByDistrAndUdD(): Unit = {
    Environment.setProperty("rcugas.sqoop.date", "20210611")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "true")

    val measureRDD: RDD[Flow] = Environment.getSpark.sparkContext.parallelize(
      List(
        Tgl(service = "TGL", pdr = "pdr", readType = None, date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None),
        Tgl(service = "TGL", pdr = "pdrPrime", readType = None, date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None),
        Tgl(service = "TGL", pdr = "pdrSecond", readType = None, date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None),
        Tgl(service = "TGL", pdr = "pdrThird", readType = None, date = None, measure = None, converted = None, serialNumberMis = None,
          pivaDistr = None, pivaUtente = None, serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = None)
      )
    )
    val sqlCtx =  Environment.getSpark.sqlContext
    import sqlCtx.implicits._
    val conn2DistrDF =  Environment.getSpark.sparkContext.parallelize(
      List(
        ("pdr", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "id1"),
        ("pdrPrime", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0", "id1"),
        ("pdrSecond", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "id3"),
        ("pdrThird", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "id2")
      )
    ).toDF(
      RcuGasConnessioniDistr2Schema.t_codice_pdr,
      RcuGasConnessioniDistr2Schema.d_data_inizio_conn,
      RcuGasConnessioniDistr2Schema.d_data_fine_conn,
      RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione,
      RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione,
      RcuGasConnessioniDistr2Schema.n_id_distr
    )
    val rcuGasMassivo =  Environment.getSpark.sparkContext.parallelize(
      List(
        ("pdr", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "piva_udd10"),
        ("pdrPrime", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0", "piva_udd9"),
        ("pdrSecond", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "piva_udd8"),
        ("pdrThird", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "piva_udd10")
      )
    ).toDF(
      RcuGasMassivoPSchema.t_codice_pdr,
      RcuGasMassivoPSchema.d_data_inizio_for,
      RcuGasMassivoPSchema.data_fine_for,
      RcuGasMassivoPSchema.piva_udd
    )
    val iFC:InclusionFilterController = new InclusionIdDistrPivaUdDFilter(rcuGasMassivo, conn2DistrDF)
    iFC.inclusionFileDf.show(false)
    val filteredMeasures = iFC.filter(measureRDD)
    filteredMeasures.collect.foreach(println)
    Assert.assertTrue(iFC.isEnabled)
    Assert.assertEquals(1, filteredMeasures.filter(_.pdr.equals("pdrThird")).count)
    Assert.assertEquals(0, filteredMeasures.filter(_.pdr.equals("pdrPrime")).count)
    Assert.assertEquals(0, filteredMeasures.filter(_.pdr.equals("pdrSecond")).count)
    Assert.assertEquals(0, filteredMeasures.filter(_.pdr.equals("pdr")).count)
  }
}
