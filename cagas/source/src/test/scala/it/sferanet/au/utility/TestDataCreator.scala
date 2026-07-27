package it.sferanet.au.utility

import it.sferanet.au.schema._
import it.sferanet.au.utilities.Environment
import it.sferanet.au.{App, EnvironmentSparkTest}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * This class is responsible of data creation for test cluster. <br><br>
 *
 * It has been inserted into test packages since it is a core part of actual test. It is also possible to run and debug
 * App.run locally with the same data using the  testAppWithFakeData method. The created entities refers to <a href="https://engit.sharepoint.com/:b:/r/sites/AcquirenteUnico2020/Documenti%20condivisi/Sviluppi%20Cloudera/Sviluppo/Calcolo%20CA%20e%20Aggiustamento/Documenti%20Tecnici/DataFlowDiagram/CaDataFlow.pdf?csf=1&web=1&e=ENJjPP">this</a> document.
 * <br><br>
 *
 * Files are created under the src/main/resources/profiles/dev/hdfs path so that at install time (mvn clean install)
 * they are produced as project assets files ready to be deployed.<br>
 */
case class TestDataCreator() extends EnvironmentSparkTest with it.sferanet.au.utility.filterPdrCreator.Creator {
  val outRootFolder = "src/main/resources/profiles/dev/hdfs"
  val warehouseFolder = "/user/hive/warehouse"
  val misureFolder = "/au.db/misure_gas_au/cmg_gas"
  val annomese = "202003"
  val mese_comp = "032020"
  //private val log = org.apache.log4j.LogManager.getLogger(this.getClass)
  initProps()

  def testAppWithFakeData(): Unit = {
    initProps()
    createMeasures()
    Environment.setProperty("flow.dataset.a01.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_A01_0150_p")
    Environment.setProperty("flow.dataset.a40.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_A40_0150_p")
    Environment.setProperty("flow.dataset.sm1.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_SM1_0150_p")
    Environment.setProperty("flow.dataset.tmv.basePath", "src/test/resources/AppTest/input/cmg_gas/prt_cmg_tmv_p")
    App.run
  }

  def testDataCreate(): Unit = {
    initProps()
    createMeasures()
  }

  override def createMeasures(): Unit = {
    //MEASURES
    createRMLMeasures()
    createRMVMeasures()
    createRSLMeasures()
    createSW1Measures()
    createTALMeasures()
    createTAVMeasures()
    createTASMeasures()
    createTMLMeasures()
    createTMVMeasures()
    createIM1Measures()
    createA010150Measures()
    createA400150Measures()
    createSM10150Measures()
    createRGLMeasures()
    createTGLMeasures()

    //AUX TABLES:
    createRcuTech() //required for consumptions [rcugas_tech_ca_p_20201230)
    createRcuGasMassivoCaP() //required for consumptions [rcugas_massivo_ca_p_20201230_b]
    createRcuGasConnessioniDistr2P() //required for pdrMassivo (hence CaFinal)
    createRcuGasBilanciamentoP() //required for pdrMassivo (hence CaFinal)
    createPrtIstatRegioneClimaticaP() //required for pdrMassivo (hence CaFinal) and CA
    createRcuGasMassivoP() //required for pdrMassivo (hence CaFinal)
    createSettleGasGasTds() // required for validation, CA and filters
    createWeights() // required for CA
    createVRcuGasDistributoreP() //required for filters
    createRcuGasUdbP() //required for filters
    createRcuAziendaP() //required for filters
    createCaFinal() //required for filters (5.3)
    createCaPreFinal()
    createRcuGasVarProfilo()
    createRcuGasVarTrattamento()
    createRcuGasVarPrelAnnuo()
  }

  def createRMLMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getRmlParquetPath
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "RML", "04/01/2020", "1", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", "RML", "01/02/2020", "1", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", "RML", "02/03/2020", "1", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", "RML", "01/01/2020", "1", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", "RML", "01/02/2020", "1", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", "RML", "02/03/2020", "1", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("RAF11", "RML", "04/01/2019", "1", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "01-01-2019T00:00:00.000000"),
      ("RAF11", "RML", "01/01/2020", "1", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "01-01-2019T00:00:00.000000"),
      ("RAF11", "RML", "02/01/2020", "1", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "01-01-2019T00:00:00.000000"),
      ("RAF11", "RML", "03/01/2020", "1", "103", "30", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "01-01-2019T00:00:00.000000"),
      ("RAF11", "RML", "04/01/2020", "1", "104", "40", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_10.xml", "01-01-2019T00:00:00.000000"),
      ("RAF11", "RML", "05/01/2020", "1", "105", "50", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151502_1.xml", "01-01-2019T00:00:00.000000"),
      ("RAF12", "RML", "04/01/2019", "1", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "01-01-2019T00:00:00.000000"),
      ("RAF12", "RML", "01/01/2020", "1", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "01-01-2019T00:00:00.000000"),
      ("RAF12", "RML", "02/01/2020", "1", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "01-01-2019T00:00:00.000000"),
      ("RAF12", "RML", "03/01/2020", "1", "104", "30", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "01-01-2019T00:00:00.000000"),
      ("RAF12", "RML", "04/01/2020", "1", "105", "40", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151501_10.xml", "01-01-2019T00:00:00.000000"),
      ("RAF12", "RML", "05/01/2020", "1", "106", "50", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0110/00489490011_12420101003_201912_TGL0050_20200109151502_1.xml", "01-01-2019T00:00:00.000000")

    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      RmlSchema.data_racc, //date - dataRacc
      RmlSchema.mot_rett_lett, //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite").mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createRMVMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getRmvParquetPath
    val servizio = "RMV"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "1", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "01/02/2020", "1", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "02/03/2020", "1", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "01/01/2020", "1", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "01/02/2020", "1", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "02/03/2020", "1", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_comp", //date - dataComp
      RmlSchema.mot_rett_lett, //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createRSLMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getRslParquetPath
    val servizio = "RSL"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "1", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "01/02/2020", "1", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "02/03/2020", "1", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "01/01/2020", "1", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "01/02/2020", "1", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "02/03/2020", "1", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_comp", //date - dataComp
      RmlSchema.mot_rett_lett, //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createSW1Measures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getSw1ParquetPath
    val servizio = "SW1"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "000000817", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000", "E", "1.0000000"),
      ("1", s"$servizio", "01/02/2020", "000000817", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1.0000000"),
      ("1", s"$servizio", "02/03/2020", "000000817", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1.0000000"),
      ("2", s"$servizio", "01/01/2020", "000000817", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000", "E", "1.0000000"),
      ("2", s"$servizio", "01/02/2020", "000000817", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1.0000000"),
      ("2", s"$servizio", "02/03/2020", "000000817", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1.0000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_deco_switch", //date - dataComp
      "segn_mis_sost",
      "segn_conv",
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento, //d_caricamento
      "tipo_lettura",
      "coeff_corr"
    )
      .withColumn("data_mis_eff", lit("01/01/2001"))
      .withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createTALMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getTalParquetPath
    val servizio = "TAL"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "V", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "01/02/2020", "V", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "02/03/2020", "V", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "01/01/2020", "V", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "01/02/2020", "V", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "02/03/2020", "V", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_com_autolet_cf", //date - dataComp
      "esito_val", //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createTAVMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getTavParquetPath
    val servizio = "TAV"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "V", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "01/02/2020", "V", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "02/03/2020", "V", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "01/01/2020", "V", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "01/02/2020", "V", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "02/03/2020", "V", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_com_autolet_cf", //date - dataComp
      "esito_val", //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createTASMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getTasParquetPath
    val servizio = "TAS"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "V", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "01/02/2020", "V", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "02/03/2020", "V", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "01/01/2020", "V", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "01/02/2020", "V", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "02/03/2020", "V", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_com_autolet_cf", //date - dataComp
      "esito_val", //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createTMLMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getTmlParquetPath
    val servizio = "TML"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "SI", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000", "E", "1"),
      ("1", s"$servizio", "01/02/2020", "SI", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1"),
      ("1", s"$servizio", "02/03/2020", "SI", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1"),
      ("2", s"$servizio", "01/01/2020", "SI", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000", "E", "1"),
      ("2", s"$servizio", "01/02/2020", "SI", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1"),
      ("2", s"$servizio", "02/03/2020", "SI", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_racc",
      "val_dato", //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento, //d_caricamento
      "tipo_lettura",
      "coeff_corr"
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createTMVMeasures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getTmvParquetPath
    val servizio = "TMV"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "000000226", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000", "E", "1"),
      ("1", s"$servizio", "01/02/2020", "000000226", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1"),
      ("1", s"$servizio", "02/03/2020", "000000226", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1"),
      ("2", s"$servizio", "01/01/2020", "000000226", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000", "E", "1"),
      ("2", s"$servizio", "01/02/2020", "000000226", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1"),
      ("2", s"$servizio", "02/03/2020", "000000226", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_att_contr",
      "segn_mis_sost", //mot_rett_lett -readType
      "segn_conv", //let_tot_prel -measure
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento, //d_caricamento
      "tipo_lettura",
      "coeff_corr"
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createIM1Measures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getIm1ParquetPath
    val servizio = "IM1"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "000000000", "1", "40031606", "1", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000", "E", "1.020333", "6", "1", "000000000", "1", "40031606", "1", "1"),
      ("1", s"$servizio", "01/02/2020", "000000000", "1", "40031606", "1", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1.020333", "6", "1", "000000000", "1", "40031606", "1", "1"),
      ("1", s"$servizio", "02/03/2020", "000000000", "1", "40031606", "1", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1.020333", "6", "1", "000000000", "1", "40031606", "1", "1"),
      ("2", s"$servizio", "01/01/2020", "000000000", "1", "40031606", "1", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000", "E", "1.020333", "6", "1", "000000000", "1", "40031606", "1", "1"),
      ("2", s"$servizio", "01/02/2020", "000000000", "1", "40031606", "1", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "E", "1.020333", "6", "1", "000000000", "1", "40031606", "1", "1"),
      ("2", s"$servizio", "02/03/2020", "000000000", "1", "40031606", "1", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "E", "1.020333", "6", "1", "000000000", "1", "40031606", "1", "1")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_esec_int",
      "POST_let_misuratore",
      "POST_let_correttore",
      "POST_matr_mis", //matr_mis -serialNumberMis
      "POST_matr_conv", //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento, //d_caricamento
      "PRE_tipo_mis",
      "POST_coeff_corr",
      "cau_int_mis",
      "cau_int_cor",
      "PRE_let_misuratore",
      "PRE_let_correttore",
      "PRE_matr_mis",
      "PRE_matr_conv",
      "PRE_coeff_corr"
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createA010150Measures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getA01ParquetPath
    val servizio = "A01"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "1", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "01/02/2020", "1", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "02/03/2020", "1", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "01/01/2020", "1", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "01/02/2020", "1", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "02/03/2020", "1", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_attivazione", //date - dataComp
      "esito", //mot_rett_lett -readType
      "segn_mis", //let_tot_prel -measure
      "segn_conv", //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createA400150Measures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getA40ParquetPath
    val servizio = "A40"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "1", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "01/02/2020", "1", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "02/03/2020", "1", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "01/01/2020", "1", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "01/02/2020", "1", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "02/03/2020", "1", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_attivazione", //date - dataComp
      "esito", //mot_rett_lett -readType
      "segn_mis", //let_tot_prel -measure
      "segn_conv", //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createSM10150Measures(): Unit = {
    val partitionBy = "annomese"
    val outpath = Environment.getSm1ParquetPath
    val servizio = "SM1"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "04/01/2020", "01/01/2020", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000", "1", "1.006407"),
      ("1", s"$servizio", "01/02/2020", "01/01/2020", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "1", "1.006407"),
      ("1", s"$servizio", "02/03/2020", "01/01/2020", "000000866", "1", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "1", "1.006407"),
      ("2", s"$servizio", "01/01/2020", "01/01/2020", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000", "1", "1.006407"),
      ("2", s"$servizio", "01/02/2020", "01/01/2020", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000", "1", "1.006407"),
      ("2", s"$servizio", "02/03/2020", "01/01/2020", "000000866", "1", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000", "1", "1.006407")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      "data_attivazione", //date - dataComp
      "data_ril", //mot_rett_lett -readType
      "segn_mis", //let_tot_prel -measure
      "segn_conv", //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento, //d_caricamento
      "esito",
      "coeff_corr"
    ).withColumn(partitionBy, lit(annomese))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createRGLMeasures(): Unit = {
    val partitionBy = "mese_comp"
    val outpath = Environment.getRglParquetPath
    val servizio = "RGL"
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", s"$servizio", "2020-01-04", "1", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-04T00:00:00.000000"),
      ("1", s"$servizio", "2020-02-01", "1", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("1", s"$servizio", "2020-03-02", "1", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000"),
      ("2", s"$servizio", "2020-01-01", "1", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-01-01T00:00:00.000000"),
      ("2", s"$servizio", "2020-02-01", "1", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "2020-02-01T00:00:00.000000"),
      ("2", s"$servizio", "2020-03-02", "1", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "2020-03-01T00:00:00.000000")
    ).toDF(
      RmlSchema.cod_pdr, //cod_pdr - pdr
      RmlSchema.cod_servizio, //cod_servizio - servizio
      RmlSchema.data_racc, //date - dataRacc
      RmlSchema.mot_rett_lett, //mot_rett_lett -readType
      RmlSchema.let_tot_prel, //let_tot_prel -measure
      RmlSchema.let_tot_conv, //let_tot_conv -converted
      RmlSchema.matr_mis, //matr_mis -serialNumberMis
      RmlSchema.matr_conv, //matr_conv -serialNumberConv
      RmlSchema.local_file, //local_file
      RmlSchema.d_caricamento //d_caricamento
    ).withColumn(partitionBy, lit(mese_comp))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }

  def createTGLMeasures(): Unit = {

    val partitionBy = "mese_comp"
    val outpath = Environment.getTglParquetPath
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "TGL", "04/01/2020", "5.1", "52", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0104/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "E", "2020-01-04T00:00:00.000000", "SI"),
      ("1", "TGL", "01/02/2020", "100", "10", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "E", "2020-02-01T00:00:00.000000", "SI"),
      ("1", "TGL", "02/03/2020", "102", "20", "A1100", "A1100", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "E", "2020-03-01T00:00:00.000000", "SI"),
      ("2", "TGL", "01/01/2020", "5.2", "52", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0101/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "E", "2020-01-01T00:00:00.000000", "SI"),
      ("2", "TGL", "01/02/2020", "102", "10", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0201/00489490011_12420101003_201912_TGL0050_20200109151501_2.xml", "E", "2020-02-01T00:00:00.000000", "SI"),
      ("2", "TGL", "02/03/2020", "103", "20", "A1101", "A1101", "/mnt/isilonshare1/GAS_INJ/TMG_00489490011/DISTRIBUTORE/TMG_00489490011_12420101003/2020/0302/00489490011_12420101003_201912_TGL0050_20200109151501_1.xml", "E", "2020-03-01T00:00:00.000000", "SI")
    ).toDF(
      "cod_pdr",
      "cod_servizio",
      "data_comp",
      "let_tot_prel",
      "let_tot_conv",
      "matr_mis",
      "matr_conv",
      "local_file",
      "tipo_lettura",
      "d_caricamento",
      "val_dato_mens"
    ).withColumn(partitionBy, lit(mese_comp))
      .coalesce(1).write.mode("overwrite")
      .partitionBy(partitionBy)
      .parquet(outpath)
  }


  override def createRcuTech(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val outpath = Environment.getRcugasTechPath

    val df: DataFrame = List(
      ("01/01/2001", "02/01/2020", "1", "2", "", "NO", "0", "0"),
      ("02/01/2020", "03/01/2020", "1", "2", "", "SI", "0", "0"),
      ("03/01/2020", "04/01/2020", "1", "2", "", "SI", "0", "0"),
      ("04/01/2020", "05/01/2021", "1", "2", "", "NO", "0", "0"),
      ("01/01/2001", "02/01/2020", "2", "2", "", "NO", "0", "0"),
      ("02/01/2020", "03/01/2020", "2", "2", "", "SI", "0", "0"),
      ("03/01/2020", "04/01/2020", "2", "2", "", "SI", "0", "0"),
      ("04/01/2020", "05/01/2021", "2", "2", "", "NO", "0", "0"),
      ("01/01/2020", "02/01/2020", "RAF11", "2", "", "NO", "0", "0"),
      ("02/01/2020", "03/01/2020", "RAF11", "2", "", "SI", "0", "0"),
      ("03/01/2020", "04/01/2020", "RAF11", "2", "", "SI", "0", "0"),
      ("04/01/2020", "05/01/2020", "RAF11", "2", "", "NO", "0", "0"),

      ("01/01/2020", "02/01/2020", "RAF12", "2", "", "NO", "0", "0"),
      ("02/01/2020", "03/01/2020", "RAF12", "2", "", "SI", "0", "0"),
      ("03/01/2020", "04/01/2020", "RAF12", "2", "", "SI", "0", "0"),
      ("04/01/2020", "05/01/2020", "RAF12", "2", "", "NO", "0", "0"))
      .toDF(RcuGasMassivoTechSchema.startDate,
        RcuGasMassivoTechSchema.endDate,
        RcuGasMassivoTechSchema.t_codice_pdr,
        RcuGasMassivoTechSchema.n_coeff_correzione, //n_coeff_correzione
        RcuGasMassivoTechSchema.t_misuratore_integrato,
        RcuGasMassivoTechSchema.t_pre_conv,
        RcuGasMassivoTechSchema.n_num_cifre_convertitore,
        RcuGasMassivoTechSchema.n_num_cifre_misuratore)
    df.coalesce(1).coalesce(1).write.mode("overwrite").mode(SaveMode.Overwrite).parquet(outpath)
  }

  override def createRcuGasMassivoCaP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val outpath = Environment.getRcugasMassivoPath

    List(("1", "1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_1", "piva_udd_1"),
      ("2", "2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_2", "piva_udd_2"),
      ("3", "3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_3", "piva_udd_3"),
      ("4", "4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_4", "piva_udd_4"),
      ("5", "5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2019", "10159", "Y", "", "14", "id_fornitura_5", "piva_udd_5"),
      ("6", "6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_6", "piva_udd_6"),
      ("7", "7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_7", "piva_udd_7"),
      ("RAF11", "RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_1", "piva_udd_1"),
      ("RAF12", "RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_1", "piva_udd_1")
    ).toDF(RcuGasMassivoCaPSchema.n_id_pdr,
      RcuGasMassivoCaPSchema.t_codice_pdr,
      RcuGasMassivoCaPSchema.d_data_inizio_for,
      RcuGasMassivoCaPSchema.data_fine_for,
      RcuGasMassivoCaPSchema.t_comune_istat_pdr,
      RcuGasMassivoCaPSchema.t_cod_profilo,
      RcuGasMassivoCaPSchema.t_anno_termico,
      RcuGasMassivoCaPSchema.n_prelievo_annuo,
      RcuGasMassivoCaPSchema.n_id_az_udd,
      RcuGasMassivoCaPSchema.t_trattamento,
      RcuGasMassivoCaPSchema.id_regione_climatica,
      RcuGasMassivoCaPSchema.n_id_fornitura,
      RcuGasMassivoCaPSchema.piva_udd)
      .withColumn(RcuGasMassivoSchema.startDate, lit("01/01/2019"))
      .withColumn(RcuGasMassivoSchema.endDate, lit("01/01/2021"))
      .withColumn(RcuGasMassivoCaPSchema.t_processo, lit(""))
      .withColumn(RcuGasMassivoCaPSchema.t_cod_cat_uso, lit(""))
      .coalesce(1).write.mode(SaveMode.Overwrite).parquet(outpath)
  }

  override def createRcuGasMassivoP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val outpath = Environment.getRcugasMassivoPPath

    List(
      ("1", "1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_1", "piva_udd_1"),
      ("2", "2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_2", "piva_udd_2"),
      ("3", "3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "", "id_fornitura_3", "piva_udd_3"),
      ("4", "4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "", "id_fornitura_4", "piva_udd_4"),
      ("5", "5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2019", "10159", "Y", "", "", "id_fornitura_5", "piva_udd_5"),
      ("6", "6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "10159", "Y", "", "", "id_fornitura_6", "piva_udd_6"),
      ("7", "7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016023", "T2B1", "2020", "10159", "Y", "", "", "id_fornitura_7", "piva_udd_7"),
      ("RAF11", "RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_1", "piva_udd_1"),
      ("RAF12", "RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "016024", "T2B1", "2020", "10159", "Y", "", "14", "id_fornitura_1", "piva_udd_1")
    ).toDF(
      RcuGasMassivoCaPSchema.n_id_pdr,
      RcuGasMassivoCaPSchema.t_codice_pdr,
      RcuGasMassivoCaPSchema.d_data_inizio_for,
      RcuGasMassivoCaPSchema.data_fine_for,
      RcuGasMassivoCaPSchema.t_comune_istat_pdr,
      RcuGasMassivoCaPSchema.t_cod_profilo,
      RcuGasMassivoCaPSchema.t_anno_termico,
      RcuGasMassivoCaPSchema.n_prelievo_annuo,
      RcuGasMassivoCaPSchema.n_id_az_udd,
      RcuGasMassivoCaPSchema.t_trattamento,
      RcuGasMassivoCaPSchema.id_regione_climatica,
      RcuGasMassivoCaPSchema.n_id_fornitura,
      RcuGasMassivoCaPSchema.piva_udd
    ).coalesce(1).write.mode("overwrite").mode(SaveMode.Overwrite).parquet(outpath)

  }

  override def createRcuGasVarProfilo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
    ).toDF(
      RcuGasVarProfiloPSchema.n_id_pdr,
      RcuGasVarProfiloPSchema.d_data_inizio,
      RcuGasVarProfiloPSchema.d_data_fine
    ).withColumn(RcuGasVarProfiloPSchema.t_cod_profilo, lit("T2B1"))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarProfiloPath)
  }

  override def createRcuGasVarTrattamento(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
    ).toDF(
      RcuGasVarTrattamentoPSchema.n_id_pdr,
      RcuGasVarTrattamentoPSchema.d_data_inizio,
      RcuGasVarTrattamentoPSchema.d_data_fine
    ).withColumn(RcuGasVarTrattamentoPSchema.t_trattamento_settlement, lit("Y"))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarTrattamentoPath)
  }

  override def createRcuGasVarPrelAnnuo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    List(
      ("1", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("2", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("3", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("4", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("5", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("6", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("7", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF11", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0"),
      ("RAF12", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0")
    ).toDF(
      RcuGasVarPrelAnnuoPSchema.n_id_pdr,
      RcuGasVarPrelAnnuoPSchema.d_data_inizio,
      RcuGasVarPrelAnnuoPSchema.d_data_fine
    ).withColumn(RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo, lit("10159"))
      .write.mode(SaveMode.Overwrite).parquet(Environment.getRcugasVarPrelAnnuoPath)
  }

  override def createRcuGasConnessioniDistr2P(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getRcugasConnessioniDistr2Path

    List(
      ("1", "1", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "id-distr-1", "", "", "", "1"),
      ("2", "2", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "id-distr-2", "", "", "", "1"),
      ("3", "3", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("4", "4", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("5", "5", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("6", "6", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("7", "7", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("RAF11", "RAF11", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1"),
      ("RAF12", "RAF12", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "", "1")
    ).toDF(RcuGasConnessioniDistr2PSchema.getValues: _*)
      .coalesce(1).write.mode("overwrite").mode(SaveMode.Overwrite).parquet(outpath)

  }

  override def createRcuGasBilanciamentoP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getRcugasBilanciamentoPath

    List(
      ("aaa", "", "150604000000000169", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", ""),
      ("2aba", "id_udb_2", "2", "", "1998-11-21 00:00:00.0", "2050-11-21 00:00:00.0", "", "", "", "", "")
    ).toDF(RcuGasBilanciamentoPSchema.getValues: _*)
      .coalesce(1).write.mode("overwrite").mode(SaveMode.Overwrite).parquet(outpath)

  }

  override def createPrtIstatRegioneClimaticaP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getIstatRegioneClimaticaPath
    val outpath2 = Environment.getLookupZonaClimaticaPath

    val df = List(
      ("016024", "A"),
      ("016025", "B"),
      ("016026", "C"),
      ("016027", "D"),
      ("016028", "E"),
      ("14", "E"),
      ("016028", "14")
    ).toDF(PrtIstatRegioneClimaticaPSchema.getValues: _*)

    df.coalesce(1).coalesce(1).write.mode("overwrite").mode(SaveMode.Overwrite).parquet(outpath)
    df.coalesce(1).coalesce(1).write.mode("overwrite").mode(SaveMode.Overwrite).parquet(outpath2)

  }

  override def createSettleGasGasTds(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getSettleGasTdsPath

    List(
      ("29/08/19 16:09:1580310593", "1", true, "C2", "1"),
      ("29/08/19 16:09:1580310593", "2", true, "C2", "1"),
      ("29/01/21 16:09:1580310593", "5", true, "C2", "1")
    ).toDF(
      SettleGasGasTdsSchema.data_creazione,
      SettleGasGasTdsSchema.cod_pdr,
      SettleGasGasTdsSchema.valid,
      SettleGasGasTdsSchema.cat_uso,
      SettleGasGasTdsSchema.classe_prelievo
    ).coalesce(1).write.mode("overwrite").parquet(outpath)
  }

  def createWeights(): Unit = {
    Environment.getSqlContext
      .read
      .parquet("src/test/resources/AppTest/input/settle_gas.db/tab_profili_giorn_std_perc")
      .coalesce(1).write
      .mode("overwrite")
      .parquet(Environment.getWeightsPath)
  }

  override def createVRcuGasDistributoreP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getRcugasDistributorePath

    List(
      ("piva1_test_filter_pdrs", "id-distr-1"),
      ("piva2_test_filter_pdrs", "id-distr-2")
    ).toDF(
      VRcuGasDistributorePSchema.t_piva,
      VRcuGasDistributorePSchema.n_id_distributore
    ).coalesce(1).write.mode(SaveMode.Overwrite).parquet(outpath)

  }

  override def createRcuAziendaP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getRcuAziendaPath

    List(
      ("piva_2_udb", "id_azienda_2")
    ).toDF(
      RcuAziendaPSchema.t_piva,
      RcuAziendaPSchema.n_id_azienda
    ).coalesce(1).write.mode(SaveMode.Overwrite).parquet(outpath)
  }

  override def createRcuGasUdbP(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getRcuGasUdbPath

    List(
      ("id_azienda_2", "id_udb_2")
    ).toDF(
      RcuGasUdbPSchema.n_id_azienda,
      RcuGasUdbPSchema.n_id_udb
    ).coalesce(1).write.mode(SaveMode.Overwrite).parquet(outpath)
  }

  override def createCaFinal(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getCaFinalPath


    var caFinalDF = List(
      ("2018", "4", "00000000000001")
    ).toDF(
      CaFinalSchema.anno_competenza,
      CaFinalSchema.codice_pdr,
      CaFinalSchema.executionid
    ).coalesce(1)
    CaFinalSchema.schema.foreach(field =>
      if ((!field.name.equals(CaFinalSchema.anno_competenza.toString)) &&
        (!field.name.equals(CaFinalSchema.codice_pdr.toString)) && (!field.name.equals(CaFinalSchema.executionid.toString)))
        caFinalDF = caFinalDF.withColumn(field.name, lit("").cast(field.dataType))
    )
    caFinalDF.write
      .partitionBy(CaFinalSchema.anno_competenza, CaFinalSchema.executionid)
      .mode(SaveMode.Append).parquet(outpath)
  }

  def createCaPreFinal(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._
    val outpath = Environment.getCaPreFinalPath


    var caPreFinalDF = List(
      ("2018", true, "4", "00000000000001", "Y"),
      ("2018", true, "RAF11", "00000000000001", "Y")
    ).toDF(
      CaPreFinalSchema.anno_competenza,
      CaPreFinalSchema.is_ca_calculated,
      CaPreFinalSchema.codice_pdr,
      CaPreFinalSchema.executionid,
      CaPreFinalSchema.trattamento
    ).coalesce(1)
    CaPreFinalSchema.schema.foreach(field =>
      if (
        (!field.name.equals(CaPreFinalSchema.trattamento.toString))
          && (!field.name.equals(CaPreFinalSchema.anno_competenza.toString))
          && (!field.name.equals(CaPreFinalSchema.is_ca_calculated.toString))
          && (!field.name.equals(CaPreFinalSchema.codice_pdr.toString))
          && (!field.name.equals(CaPreFinalSchema.executionid.toString))
      )
        caPreFinalDF = caPreFinalDF.withColumn(field.name, lit("").cast(field.dataType))
    )
    caPreFinalDF.write
      .partitionBy(CaPreFinalSchema.anno_competenza, CaPreFinalSchema.executionid)
      .mode(SaveMode.Append).parquet(outpath)
  }

  def initProps(): Unit = {
    //OVERRIDE DEFAULT CONFIGURATIONS
    //Environment.resetProperty()
    Environment.setProperty("flow.dataset.rgl.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_rgl_p")
    Environment.setProperty("flow.dataset.rml.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_rml_p")
    Environment.setProperty("flow.dataset.rmv.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_rmv_p")
    Environment.setProperty("flow.dataset.rsl.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_rsl_p")
    Environment.setProperty("flow.dataset.sw1.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_sw1_p")
    Environment.setProperty("flow.dataset.tal.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_tal_p")
    Environment.setProperty("flow.dataset.tav.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_tav_p")
    Environment.setProperty("flow.dataset.tas.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_tas_p")
    Environment.setProperty("flow.dataset.tgl.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_tgl_p")
    Environment.setProperty("flow.dataset.tml.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_tml_p")
    Environment.setProperty("flow.dataset.tmv.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_tmv_p")
    Environment.setProperty("flow.dataset.im1.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_im1_p")
    Environment.setProperty("flow.dataset.a01.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_A01_0150_p")
    Environment.setProperty("flow.dataset.a40.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_A40_0150_p")
    Environment.setProperty("flow.dataset.sm1.basePath", s"$outRootFolder$warehouseFolder$misureFolder/prt_cmg_SM1_0150_p")
    Environment.setProperty("weights.basepath", s"$outRootFolder$warehouseFolder/settle_gas.db/tab_profili_giorn_std_perc")
    Environment.setProperty("rcugas_tech.basepath", s"$outRootFolder$warehouseFolder/rcugas.db/rcugas_tech_ca_p")
    Environment.setProperty("rcugas.basepath", s"$outRootFolder$warehouseFolder/rcugas.db/rcugas_massivo_ca_p")
    Environment.setProperty("tds.basepath", s"$outRootFolder$warehouseFolder/settle_gas.db/gas_tds_part")
    Environment.setProperty("lookup_zonaclimatica.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop/prt.istat_regione_climatica_p")
    Environment.setProperty("v_rcugas_distributore_p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/rcugas.v_rcugas_distributore_p")
    Environment.setProperty("validation.dataset.basePath", s"$outRootFolder$warehouseFolder/apps/CA/output/validation/executionid=%d")
    Environment.setProperty("consumption.dataset.basePath", s"$outRootFolder$warehouseFolder/apps/CA/output/consumption/executionid=%d")
    Environment.setProperty("ca.dataset.basePath", s"$outRootFolder$warehouseFolder/apps/CA/output/ca/executionid=%d")
    Environment.setProperty("validation.basePath", s"$outRootFolder$warehouseFolder/apps/CA/output/validation")
    Environment.setProperty("consumption.basePath", s"$outRootFolder$warehouseFolder/apps/CA/output/consumption")
    Environment.setProperty("ca.basePath", s"$outRootFolder$warehouseFolder/apps/CA/output/ca")
    Environment.setProperty("rcugas_massivo__p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/RCUGAS.RCUGAS_MASSIVO_p")
    Environment.setProperty("rcugas_connessioni_distr2__p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/RCUGAS.RCUGAS_CONNESSIONI_DISTR2_p")
    Environment.setProperty("rcugas_bilanciamento_p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/RCUGAS.RCUGAS_BILANCIAMENTO_p")
    Environment.setProperty("istat_regione_climatica_p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/prt.istat_regione_climatica_p")
    Environment.setProperty("rcugas_udb_p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/rcugas.rcugas_udb_p")
    Environment.setProperty("rcu_azienda_p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/rcu.rcu_azienda_p")
    Environment.setProperty("gas_tds.basepath", s"$outRootFolder$warehouseFolder/settle_gas.db/gas_tds_part")
    Environment.setProperty("ca_pre_final.basepath", s"$outRootFolder$warehouseFolder/CA/output/ca_pre_final")
    Environment.setProperty("ca_final.basepath", s"$outRootFolder$warehouseFolder/CA/output/ca_final")
    Environment.setProperty("ca_final_to_export.basepath", s"$outRootFolder$warehouseFolder/user/development/test/ca_final_to_export")

    Environment.setProperty("rcugas.var_trattamento.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/RCUGAS.RCUGAS_VAR_TRATTAMENTO_p")
    Environment.setProperty("rcugas.rcugas_var_prel_annuo_p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/RCUGAS.RCUGAS_VAR_PREL_ANNUO_p")
    Environment.setProperty("rcugas.rcugas_var_profilo_p.basepath", s"$outRootFolder$warehouseFolder/acquirente_unico/sqoop_portcons/RCUGAS.RCUGAS_VAR_PROFILO_p")
  }
}
