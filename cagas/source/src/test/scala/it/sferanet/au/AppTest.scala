package it.sferanet.au

import it.sferanet.au.schema.{CaFinalSchema, CaPreFinalSchema}
import it.sferanet.au.utilities.{Environment, HDFSUtils}
import it.sferanet.au.utility.filterPdr.filter7ExclusionFilePdrTest.check
import it.sferanet.au.utility.filterPdrCreator.CreatorFactory
import org.apache.spark.sql.functions.{col, count, lit, max}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.Assert

class AppTest extends EnvironmentSparkTest {
  //Environment.resetProperty()

  /**
   * Legacy code test
   * */
  def testApp(): Unit = {
    //Environment.resetProperty()

    Environment.setProperty("flow.read.startDate", "201810")
    Environment.setProperty("flow.read.endDate", "202005")

    Environment.setProperty("filterPdr.mode", "")
    Environment.setProperty("ignorePdrMeasure.enable", "false")

    Environment.setProperty("flow.dataset.a01.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_A01_0150_p")
    Environment.setProperty("flow.dataset.a40.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_A40_0150_p")
    Environment.setProperty("flow.dataset.sm1.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_SM1_0150_p")
    //Create entities not required from initial CA procedure
    val c = it.sferanet.au.utility.filterPdrCreator.appTest.Creator()
    c.createRcuGasMassivoP()
    c.createPrtIstatRegioneClimaticaP()
    c.createRcuGasBilanciamentoP()
    c.createRcuGasConnessioniDistr2P()
    c.createRcuGasVarPrelAnnuo()
    c.createRcuGasVarTrattamento()
    c.createRcuGasVarProfilo()

    App.run()

    val sqlContext = SQLContext.getOrCreate(null)

    val validation = sqlContext.read.parquet("src/test/resources/AppTest/output/validation")
      .drop("motivazione_rettifica")
      .drop("cau_int_mis")
      .drop("cau_int_cor")
      .drop("n_coeff_correzione")
    val validationExpected = sqlContext.read.parquet("src/test/resources/AppTest/expected/validation")
    Assert.assertTrue(validation.except(validationExpected).union(validationExpected.except(validation)).count() == 0)

    val consumption = sqlContext.read.parquet("src/test/resources/AppTest/output/consumption")
      .drop("tipo_coeff")
      .drop("tipo_forzatura")
      .drop("coerenza_dim")
      .drop("end_t_misuratore_integrato")
      .drop("end_t_pre_conv")
    val consumptionExpected = sqlContext.read.parquet("src/test/resources/AppTest/expected/consumption")
    Assert.assertTrue(consumption.except(consumptionExpected).union(consumptionExpected.except(consumption)).count() == 0)

    val ca = sqlContext.read.parquet("src/test/resources/AppTest/output/ca")
    val caExpected = sqlContext.read.parquet("src/test/resources/AppTest/expected/ca")
    Assert.assertTrue(ca.except(caExpected).union(caExpected.except(ca)).count() == 0)

  }

  def testFilterModePdr(): Unit = {
    //Environment.resetProperty()

    val rootPath = "src/test/resources/AppTest/filterModePdr"

    //SET TABLES PATH
    setEnvForFilterTest(rootPath)

    //SET FILTER MODE
    Environment.setProperty("filterPdr.mode", "pdr")


    //CREATE REQUIRED ENTITIES
    cleanOutDir(rootPath)
    val creator = CreatorFactory.getTestCreator(CreatorFactory.appTest)

    //RUN TEST AND VERIFY
    App.run()
    val caPreFinalFilterPdr = getCaPreFinalDF()
    caPreFinalFilterPdr.show()
    Assert.assertEquals(1, caPreFinalFilterPdr.where(col(CaPreFinalSchema.codice_pdr) === "1").select(CaPreFinalSchema.codice_pdr).distinct.count())

    caFinalUniquePdrTest()
  }

  def testFilterModeDistributore(): Unit = {
    //Environment.resetProperty()

    val rootPath = "src/test/resources/AppTest/FilterModeDistributore"
    //SET TABLES PATH
    setEnvForFilterTest(rootPath)

    //CREATE REQUIRED ENTITIES
    cleanOutDir(rootPath)
    CreatorFactory.getTestCreator(CreatorFactory.appTest)

    //SET FILTER MODE
    Environment.setProperty("filterPdr.mode", "distributore")
    App.run()
    val caPreFinalFilterDistr = getCaPreFinalDF()
    caPreFinalFilterDistr.show()
    Assert.assertEquals(1, caPreFinalFilterDistr.where(col(CaPreFinalSchema.codice_pdr) === "2").select(CaPreFinalSchema.codice_pdr).distinct.count())

    caFinalUniquePdrTest()
  }

  def testFilterModeDistrUdd(): Unit = {
    //Environment.resetProperty()

    val rootPath = "src/test/resources/AppTest/FilterModeDistrUdd"
    //SET TABLES PATH
    setEnvForFilterTest(rootPath)

    //CREATE REQUIRED ENTITIES
    cleanOutDir(rootPath)
    CreatorFactory.getTestCreator(CreatorFactory.appTest)


    //SET FILTER MODE
    Environment.setProperty("filterPdr.mode", "distr-udd")
    App.run()
    val caPreFinalFilterDistrUdd = getCaPreFinalDF()
    Assert.assertEquals(1, caPreFinalFilterDistrUdd.where(col(CaPreFinalSchema.codice_pdr) === "3").select(CaPreFinalSchema.codice_pdr).distinct.count())

    caFinalUniquePdrTest()
  }

  def testFilterModeDistrUddUdb(): Unit = {
    //Environment.resetProperty()

    val rootPath = "src/test/resources/AppTest/FilterModeDistrUddUdb"
    //SET TABLES PATH
    setEnvForFilterTest(rootPath)

    //CREATE REQUIRED ENTITIES
    cleanOutDir(rootPath)
    CreatorFactory.getTestCreator(CreatorFactory.appTest)


    //SET FILTER MODE
    Environment.setProperty("filterPdr.mode", "distr-udd-udb")
    App.run()
    val caPreFinalFilterDistrUddUdb = getCaPreFinalDF()
    caPreFinalFilterDistrUddUdb.show()
    Assert.assertEquals(1, caPreFinalFilterDistrUddUdb.where(col(CaPreFinalSchema.codice_pdr) === "4").select(CaPreFinalSchema.codice_pdr).distinct.count())

    caFinalUniquePdrTest()
  }

  def testFilterModeOggettoVariazione(): Unit = {
    //Environment.resetProperty()

    val rootPath = "src/test/resources/AppTest/FilterModeOggettoVariazione"
    //SET TABLES PATH
    setEnvForFilterTest(rootPath)

    //CREATE REQUIRED ENTITIES
    cleanOutDir(rootPath)
    CreatorFactory.getTestCreator(CreatorFactory.appTest)
    //SET FILTER MODE
    Environment.setProperty("filterPdr.mode", "oggettoVariazione")
    App.run()
    val caPreFinalFilterOggettoVariazione = getCaPreFinalDF()
    caPreFinalFilterOggettoVariazione.show()
    //    Assert.assertEquals(1, caPreFinalFilterOggettoVariazione.where(col(CaPreFinalSchema.codice_pdr) === "5.1").select(CaPreFinalSchema.codice_pdr).distinct.count())

    caFinalUniquePdrTest()
  }

  def testFilterModeCaricamentoTDS(): Unit = {
    //Environment.resetProperty()

    val rootPath = "src/test/resources/AppTest/FilterModeCaricamentoTDS"
    //SET TABLES PATH
    setEnvForFilterTest(rootPath)

    //CREATE REQUIRED ENTITIES
    cleanOutDir(rootPath)
    CreatorFactory.getTestCreator(CreatorFactory.appTest)

    //SET FILTER MODE
    Environment.setProperty("filterPdr.mode", "caricamentoTDS")
    App.run()
    val caPreFinalFilterCaricamentoTds = getCaPreFinalDF()
    caPreFinalFilterCaricamentoTds.show()
    //    Assert.assertEquals(1, caPreFinalFilterCaricamentoTds.where(col(CaPreFinalSchema.codice_pdr) === "5.2").select(CaPreFinalSchema.codice_pdr).distinct.count())

    caFinalUniquePdrTest()
  }

  def testFilterModeNoMisure(): Unit = {
    //Environment.resetProperty()

    val rootPath = "src/test/resources/AppTest/FilterModeNoMisure"
    //SET TABLES PATH
    setEnvForFilterTest(rootPath)

    //CREATE REQUIRED ENTITIES
    cleanOutDir(rootPath)
    CreatorFactory.getTestCreator(CreatorFactory.appTest)

    Environment.setProperty("filterPdr.mode", "noMisure")
    App.run()
    val caPreFinalFilterNoMisure = getCaPreFinalDF().cache()
    caPreFinalFilterNoMisure.show()
    Assert.assertEquals(1, caPreFinalFilterNoMisure.where(col(CaPreFinalSchema.codice_pdr) === "5.3").select(CaPreFinalSchema.codice_pdr).distinct.count())

    caFinalUniquePdrTest()
  }

  def testSimpleCompleteExecution(): Unit = {
    //Environment.resetProperty()

    val resourcesInputPath = "src/test/resources/inputParamFile/complete/SampleCalculateFinalCaTest"

    setDefaultEnvForRaf(resourcesInputPath)

    Environment.setProperty("filterPdr.mode", "")
    Environment.setProperty("ignorePdrMeasure.enable", "false")


    HDFSUtils.deleteIfExist(Environment.getCaPreFinalPath)

    CreatorFactory.getTestCreator(CreatorFactory.simpleCalculateFinalCaTest)
    App.run()
    val sqlContext = Environment.getSqlContext

    val validation: DataFrame = sqlContext.read.parquet(f"$resourcesInputPath/validation")
    validation.show()
    val consumption = sqlContext.read.parquet(f"$resourcesInputPath/consumption")
    consumption.show()
    val ca = sqlContext.read.parquet(f"$resourcesInputPath/ca")
    ca.show()
    val caPreFinal = sqlContext.read.parquet(f"$resourcesInputPath/ca_pre_final")
    caPreFinal.show()

    ca.show()
    caPreFinal.show()

    val caExpected1: Long = 99
    val caExpected2: Long = 100

    Assert.assertEquals(caExpected1, ca.filter(col("pdr") === lit(1)).select("ca_sum").distinct().collect()(0)(0))
    //    Assert.assertEquals(caExpected2, ca.filter(col("pdr") === lit(2)).select("ca_sum").distinct().collect()(0)(0))

    caFinalUniquePdrTest()

  }

  def testFilter6CalculateCaFinalForcingTest(): Unit = {
    //Environment.resetProperty()

    val sqlContext = Environment.getSqlContext
    val resourcesInputPath = "src/test/resources/inputParamFile/complete/Filter6CalculateCaFinalForcingTest"

    setDefaultEnvForRaf(resourcesInputPath)

    Environment.setProperty("filterPdr.mode", "forzatura")
    Environment.setProperty("filterPdr.forzatura.path", f"$resourcesInputPath/pdr_forzatura.csv")


    HDFSUtils.deleteIfExist(Environment.getCaPreFinalPath)

    CreatorFactory.getTestCreator(CreatorFactory.filter6CalculateCaFinalForcingTest)
    App.run()
    val validation: DataFrame = sqlContext.read.parquet(f"$resourcesInputPath/validation")
    validation.show()
    val consumption = sqlContext.read.parquet(f"$resourcesInputPath/consumption")
    consumption.show()
    val ca = sqlContext.read.parquet(f"$resourcesInputPath/ca")
    ca.show()
    val caPreFinal = sqlContext.read.parquet(f"$resourcesInputPath/ca_pre_final")
    caPreFinal.show()

    /**
     * controllo l'esistenza dei pdr in base al filtro
     */
    check.checkIfExsist(caPreFinal, excepted = true, "1")
    check.checkIfExsist(caPreFinal, excepted = true, "2")
    check.checkIfExsist(caPreFinal, excepted = false, "3")
    check.checkIfExsist(caPreFinal, excepted = true, "4")
    check.checkIfExsist(caPreFinal, excepted = false, "5")
    check.checkIfExsist(caPreFinal, excepted = false, "6")
    check.checkIfExsist(caPreFinal, excepted = true, "7")
    /** *
     * controllo il valore della ca
     */
    val caExpected1: Long = 99
    val caExpected2: Long = 100

    Assert.assertEquals(caExpected1, ca.filter(col("pdr") === lit(1)).select("ca_sum").distinct().collect()(0)(0))
    //    Assert.assertEquals(caExpected2, ca.filter(col("pdr") === lit(2)).select("ca_sum").distinct().collect()(0)(0))

    /**
     * controllo che i pdr nel file della forzatura siano presenti nella caPreFinal e che abbiano i valori costanti del file
     * pdr_forzatura.csv
     * PDR,CA,COD_PREL
     * 1,12.0,a1
     * 2,12.2,a12
     * 4,14.0,a3
     * 7,15.0,a4
     */

    Assert.assertEquals(12.0.toString, caPreFinal.filter(col("codice_pdr") === lit(1)).select("prelievo_annuo_prev_forced").distinct().collect()(0)(0))
    Assert.assertEquals("C1A1", caPreFinal.filter(col("codice_pdr") === lit(1)).select("cod_prof_prel_std_forced").distinct().collect()(0)(0))

    Assert.assertEquals(12.2.toString, caPreFinal.filter(col("codice_pdr") === lit(2)).select("prelievo_annuo_prev_forced").distinct().collect()(0)(0))
    Assert.assertEquals("C1B1", caPreFinal.filter(col("codice_pdr") === lit(2)).select("cod_prof_prel_std_forced").distinct().collect()(0)(0))

    Assert.assertEquals(14.0.toString, caPreFinal.filter(col("codice_pdr") === lit(4)).select("prelievo_annuo_prev_forced").distinct().collect()(0)(0))
    Assert.assertEquals("C2X1", caPreFinal.filter(col("codice_pdr") === lit(4)).select("cod_prof_prel_std_forced").distinct().collect()(0)(0))

    Assert.assertEquals(15.0.toString, caPreFinal.filter(col("codice_pdr") === lit(7)).select("prelievo_annuo_prev_forced").distinct().collect()(0)(0))
    Assert.assertEquals("T2D2", caPreFinal.filter(col("codice_pdr") === lit(7)).select("cod_prof_prel_std_forced").distinct().collect()(0)(0))

    /*   Assert.assertEquals(2,
         caPreFinal.where(col(CaPreFinalSchema.codice_pdr) === lit(11))
           .where(col(CaPreFinalSchema.cod_prof_prel_std_forced) === lit("C2X1"))
           .count()
       )*/

    caFinalUniquePdrTest()

  }

  def testMeasureFilter(): Unit = {
    //Environment.resetProperty()

    Environment.setProperty("flow.read.startDate", "201810")
    Environment.setProperty("flow.read.endDate", "202005")

    val sqlContext = Environment.getSqlContext

    val resourcesInputPath = "src/test/resources/AppTest/MeasureFilter"

    setDefaultEnvForRaf(resourcesInputPath)

    Environment.setProperty("ignorePdrMeasure.enable", "true")
    Environment.setProperty("ignorePdrMeasure.pdrMeasureFile.path", f"$resourcesInputPath/pdrAndMeasuresToExclude.csv")

    HDFSUtils.deleteIfExist(Environment.getCaPreFinalPath)
    HDFSUtils.deleteIfExist(Environment.getCaPreFinalPath)
    CreatorFactory.getTestCreator(CreatorFactory.filter7ExclusioneFilePdrCaFinalTest)

    App.run()

    //Checking that only A010150 are going to be considered
    val validation: DataFrame = sqlContext.read.parquet(f"$resourcesInputPath/validation")
    validation.show()

    var serviceCol = col("service")
    Assert.assertEquals(0,
      validation
        .where(serviceCol =!= lit("A01"))
        .count()
    )
    Assert.assertNotEquals(0,
      validation
        .where(serviceCol === lit("A01"))
        .count()
    )

    caFinalUniquePdrTest()
  }

  private def getCaPreFinalDF(): DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getCaPreFinalPath)
  }

  private def getCaFinalDF(): DataFrame = {
    Environment.getSqlContext.read.parquet(Environment.getCaFinalPath)
  }

  /**
   * The test is always the same, the result might change accordig to the caFinal we are considering. Remember that it
   * might be different from test case to test case depending on the value of  ca_final.basepath.
   * */
  private def caFinalUniquePdrTest(): Unit = {
    val caFinal = getCaFinalDF().cache()
    caFinal.show()

    val lastCaFinalPdrCountDF = caFinal
      .join(caFinal.select(max(col(CaFinalSchema.executionid)).as(CaFinalSchema.executionid)), Seq(CaFinalSchema.executionid.toString), "inner")
      .select(CaFinalSchema.codice_pdr)
      .groupBy(col(CaFinalSchema.codice_pdr))
      .agg(count("*").alias("nr_pdr"))
      .cache()

    Assert.assertEquals(0, lastCaFinalPdrCountDF.where(col("nr_pdr") > lit(1)).count())
    Assert.assertNotEquals(0, lastCaFinalPdrCountDF.where(col("nr_pdr") === lit(1)).count())
  }

  private def setEnvForFilterTest(rootPath: String): Unit = {
    //SET TEST TABLES PATHS
    Environment.setProperty("rcugas.basepath", s"$rootPath/rcugas.db/rcugas_massivo_ca_p")
    Environment.setProperty("rcugas_massivo__p.basepath", s"$rootPath/RCUGAS.RCUGAS_MASSIVO_p")
    Environment.setProperty("rcugas_connessioni_distr2__p.basepath", s"$rootPath/RCUGAS.RCUGAS_CONNESSIONI_DISTR2_p")
    Environment.setProperty("rcugas_bilanciamento_p.basepath", s"$rootPath/RCUGAS.RCUGAS_BILANCIAMENTO_p")
    Environment.setProperty("istat_regione_climatica_p.basepath", s"$rootPath/prt.istat_regione_climatica_p")
    Environment.setProperty("rcugas_udb_p.basepath", s"$rootPath/rcugas.rcugas_udb_p")
    Environment.setProperty("rcu_azienda_p.basepath", s"$rootPath/rcu.rcu_azienda_p")
    Environment.setProperty("gas_tds.basepath", s"$rootPath/gas_tds_part")
    Environment.setProperty("ca_pre_final.basepath", s"$rootPath/ca_pre_final")
    Environment.setProperty("ca_final.basepath", s"$rootPath/ca_final")
    Environment.setProperty("ca.basePath", s"$rootPath/ca")
    Environment.setProperty("consumption.basepath", s"$rootPath/consumption")
    Environment.setProperty("validation.basepath", s"$rootPath/validation")
    Environment.setProperty("v_rcugas_distributore_p.basepath", s"$rootPath/v_rcugas_distributore_p")
    Environment.setProperty("rcugas.var_trattamento.basepath", s"$rootPath/RCUGAS.RCUGAS_VAR_TRATTAMENTO_p")
    Environment.setProperty("rcugas.rcugas_var_prel_annuo_p.basepath", s"$rootPath/RCUGAS.RCUGAS_VAR_PREL_ANNUO_p")
    Environment.setProperty("rcugas.rcugas_var_profilo_p.basepath", s"$rootPath/RCUGAS.RCUGAS_VAR_PROFILO_p")
  }

  private def cleanOutDir(root: String): Unit = {
    HDFSUtils.deleteIfExist(root)
  }

  /** fixme: capire se si può accorpare con setEnvForFilterTest */
  private def setDefaultEnvForRaf(resourcesInputPath: String): Unit = {
    //SET TEST TABLES PATHS
    Environment.setProperty("flow.dataset.a01.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_A01_0150_p")
    Environment.setProperty("flow.dataset.rml.basePath", f"$resourcesInputPath/prt_cmg_rml_p")
    Environment.setProperty("rcugas.basepath", f"$resourcesInputPath/rcugas")
    Environment.setProperty("rcugas_tech.basepath", f"$resourcesInputPath/rcugas_tech")
    Environment.setProperty("validation.dataset.basePath", f"$resourcesInputPath/validation")
    Environment.setProperty("consumption.dataset.basePath", f"$resourcesInputPath/consumption")
    Environment.setProperty("ca.dataset.basePath", f"$resourcesInputPath/ca")
    Environment.setProperty("ca_pre_final.basepath", f"$resourcesInputPath/ca_pre_final")
    Environment.setProperty("ca_final.basepath", s"$resourcesInputPath/ca_final")
    Environment.setProperty("pdr_massivo.anno_competenza", "2021")
    Environment.setProperty("rcugas_massivo__p.basepath", f"$resourcesInputPath/rcugas_massivo__p.basepath")
    Environment.setProperty("rcugas_connessioni_distr2__p.basepath", f"$resourcesInputPath/rcugas_connessioni_distr2__p.basepath")
    Environment.setProperty("rcugas_bilanciamento_p.basepath", f"$resourcesInputPath/rcugas_bilanciamento_p.basepath")
    Environment.setProperty("istat_regione_climatica_p.basepath", f"$resourcesInputPath/istat_regione_climatica_p.basepath")

    Environment.setProperty("rcugas.var_trattamento.basepath", s"$resourcesInputPath/RCUGAS.RCUGAS_VAR_TRATTAMENTO_p")
    Environment.setProperty("rcugas.rcugas_var_prel_annuo_p.basepath", s"$resourcesInputPath/RCUGAS.RCUGAS_VAR_PREL_ANNUO_p")
    Environment.setProperty("rcugas.rcugas_var_profilo_p.basepath", s"$resourcesInputPath/RCUGAS.RCUGAS_VAR_PROFILO_p")
  }
}