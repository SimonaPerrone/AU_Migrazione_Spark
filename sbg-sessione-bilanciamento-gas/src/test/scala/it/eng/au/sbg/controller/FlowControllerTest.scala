package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.controller.FlowController
import it.eng.au.aggiustamentoGas.dao.measure._
import it.eng.au.aggiustamentoGas.filter.exclusion.ExclusionFilterController
import it.eng.au.aggiustamentoGas.filter.inclusion.InclusionFilterController
import it.eng.au.aggiustamentoGas.model.agg.PdrWithMonthTreatmentYSBG
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, IgmgPost, IgmgPre, Im1, Im1Post, Im1Pre}
import it.eng.au.aggiustamentoGas.model.measure.{A01, Flow, Rgl, Rml, Tgl}
import it.eng.au.aggiustamentoGas.schema.agg.ValidatedFlowsSchema.service
import it.eng.au.aggiustamentoGas.schema.rcugas._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import it.eng.au.sbg.dao.measure.{RglDAOSbg, RmlDAOSbg, TglDAOSbg}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class FlowControllerTest extends EnvironmentSparkTest {
  def testGet(): Unit = {
    Environment.setProperty("flow.read.ghigliottina", "20220909")
    Environment.setProperty("filter.exclusion.enabled", "false")
    Environment.setProperty("filter.strongExclusion.enabled", "false")
    Environment.setProperty("filter.inclusion.pdr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "false")

    val result = new FlowControllerCommonMock(new ExclusionFilterController(isStrong = false), new ExclusionFilterController(isStrong = true), List(): List[InclusionFilterController])
      .getAllOtherMeasures("202101", "202101", getTreatment = false).cache

    result.collect().foreach(println)

    Assert.assertEquals(3, result.count())
  }

  def testIntegrationExclusionFilterController(): Unit = {
    //using same data as testGet but with filter enabled on local_file reads less measures
    Environment.setProperty("filter.exclusion.enabled", "true")
    Environment.setProperty("filter.strongExclusion.enabled", "true")
    Environment.setProperty("flow.read.ghigliottina", "20220909")
    Environment.setProperty("filter.inclusion.pdr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "false")

    val result = new FlowControllerCommonMock(new ExclusionFilterController(isStrong = false), new ExclusionFilterController(isStrong = true), List(): List[InclusionFilterController])
      .getAllOtherMeasures("202101", "202101", getTreatment = false).cache

    result.collect().foreach(println)

    Assert.assertEquals(0, result.count())
  }

  def testIntegrationStrongExclusionFilterController(): Unit = {
    //using same data as testGet but with filter enabled on local_file reads less measures
    Environment.setProperty("filter.exclusion.enabled", "false")
    Environment.setProperty("filter.strongExclusion.enabled", "true")
    Environment.setProperty("flow.read.ghigliottina", "20220909")
    Environment.setProperty("filter.inclusion.pdr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "false")

    val result = new FlowControllerCommonMock(new ExclusionFilterController(isStrong = false), new ExclusionFilterController(isStrong = true), List(): List[InclusionFilterController])
      .getAllOtherMeasures("202101", "202101", getTreatment = false).cache

    result.collect().foreach(println)

    Assert.assertEquals(0, result.count())
  }

  def testIntegrationIncludeFilterController(): Unit = {
    //using same data as testGet but with filter enabled
    Environment.setProperty("filter.exclusion.enabled", "false")
    Environment.setProperty("filter.strongExclusion.enabled", "false")
    Environment.setProperty("filter.inclusion.pdr.enabled", "true")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "true")

    Environment.setProperty("flow.read.ghigliottina", "20220909")
    Environment.setProperty("rcugas.sqoop.date", "20210611")

    val sqlCtx = Environment.getSpark.sqlContext
    import sqlCtx.implicits._
    val conn2DistrDF = Environment.getSpark.sparkContext.parallelize(
      List(
        ("1", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "id2", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0"),
        ("2", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0", "id1", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0"),
        ("2", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "id3", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0"),
        ("3", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "id1", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0")
      )
    ).toDF(
      RcuGasConnessioniDistr2Schema.t_codice_pdr,
      RcuGasConnessioniDistr2Schema.d_data_inizio_conn,
      RcuGasConnessioniDistr2Schema.d_data_fine_conn,
      RcuGasConnessioniDistr2Schema.n_id_distr,
      RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione,
      RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione
    )
    val rcuGasMassivo = Environment.getSpark.sparkContext.parallelize(
      List(
        ("1", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "piva_udd10"),
        ("1", "2021-05-11 00:00:00.0", "2021-06-10 00:00:00.0", "piva_udd9"),
        ("2", "2021-06-11 00:00:00.0", "2021-06-11 00:00:00.0", "piva_udd8")
      )
    ).toDF(
      RcuGasMassivoPSchema.t_codice_pdr,
      RcuGasMassivoPSchema.d_data_inizio_for,
      RcuGasMassivoPSchema.data_fine_for,
      RcuGasMassivoPSchema.piva_udd
    )

    val result = new FlowControllerCommonMock(new ExclusionFilterController(isStrong = false), new ExclusionFilterController(isStrong = true), InclusionFilterController.getFilter(rcuGasMassivo, conn2DistrDF))
      .getAllOtherMeasures("202101", "202101", getTreatment = false).cache

    result.collect().foreach(println)

    Assert.assertEquals(2, result.filter(_.pdr.equals("1")).count())
    Assert.assertEquals(0, result.filter(!_.pdr.equals("1")).count())
  }

  class FlowControllerCommonMock(
                                  private val exclusionFilterController: ExclusionFilterController,
                                  private val strongExclusionFilterController: ExclusionFilterController,
                                  private val inclusionFilters: List[InclusionFilterController]
                                ) extends FlowControllerSbg(exclusionFilterController, strongExclusionFilterController, inclusionFilters) {
    override val listDAO: List[MeasureDAO] = List(
      new Im1DAOMock,
      new RglDAOMock
    )
  }

  val formatter = DateTimeFormat.forPattern("dd/MM/yyyy")

  class Im1DAOMock extends Im1DAO {
    override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
      val file2exclude = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")
      Environment.getSpark.sparkContext.parallelize(List(
        Im1("IM1", "1", Some(formatter.parseDateTime("01/01/2021")), None, None, None, None, None, None, None, None, None, None, file2exclude, None, Im1Pre(
          "IM1Pre", "1", Some(formatter.parseDateTime("01/01/2021")), None, Some(1), None, None, None, None, None, None, None, None, None, None
        ), null),
        //non-valid pdrs are filtered out
        Im1("IM1", "2", Some(formatter.parseDateTime("01/01/2021")), None, None, None, None, None, None, None, None, None, None, file2exclude, None, Im1Pre(
          "IM1Pre", "1", Some(formatter.parseDateTime("01/01/2021")), None, None, None, None, None, None, None, None, None, None, None, None
        ), Im1Post(
          "IM1Pre", "1", Some(formatter.parseDateTime("01/01/2021")), None, None, Some(1), None, None, None, None, None, None, None, None, None
        )),
        Im1("IM1", "  null  ", Some(formatter.parseDateTime("01/01/2021")), None, None, None, None, None, None, None, None, None, None, file2exclude, None, null, null),
        Im1("IM1", null, Some(formatter.parseDateTime("01/01/2021")), None, None, None, None, None, None, None, None, None, file2exclude, None, None, null, null)
      ))
    }
  }

  class RglDAOMock extends RglDAOSbg {
    override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
      val file2exclude = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")
      val fileWithDCaricamentoNotValid = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2023/0110/0_1_201912_TGL0050_20200109151501_1.xml")
      Environment.getSpark.sparkContext.parallelize(List(
        Rgl("RGL", "1", Some(formatter.parseDateTime("01/01/2021")), None, Some(1), None, None, None, None, Some(0), file2exclude, None),
        Rgl("RGL", "1", Some(formatter.parseDateTime("01/01/2021")), None, Some(1), None, None, None, None, Some(0), fileWithDCaricamentoNotValid, None)
      ))
    }
  }

  class FlowControllerCommonTreatmentMock(
                                           private val exclusionFilterController: ExclusionFilterController,
                                           private val strongExclusionController: ExclusionFilterController,
                                           private val inclusionFilters: List[InclusionFilterController]
                                         ) extends FlowControllerSbg(exclusionFilterController, strongExclusionController, inclusionFilters) {
    override val listDAO: List[MeasureDAO] = List(
      new TglDAOTreatMock
    )

    class TglDAOTreatMock extends TglDAOSbg {
      override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
        Environment.getSpark.sparkContext.parallelize(List(
          Tgl(service = "TGL", pdr = "pdr", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None,
            serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"))
          , Tgl(service = "TGL", pdr = "pdr2", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(3.0), converted = None,
            serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"))
          , Tgl(service = "TGL", pdr = "pdr3", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(3.0), converted = None,
            serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"))
          , Tgl(service = "TGL", pdr = "pdr4", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(3.0), converted = None,
            serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"))
        ))
      }
    }
  }

  def testTreatmentY(): Unit = {
    Environment.setProperty("flow.read.ghigliottina", "20220909")
    Environment.setProperty("filter.exclusion.enabled", "false")
    Environment.setProperty("filter.strongExclusion.enabled", "false")
    Environment.setProperty("filter.inclusion.pdr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "false")

    Environment.setProperty("filter.duplicateMeasures.enable", "false")

    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val ds = sqlContext.createDataset(List(
      PdrWithMonthTreatmentYSBG("pdr2")
      , PdrWithMonthTreatmentYSBG("pdr3")
    ))

    val result = new FlowControllerCommonTreatmentMock(new ExclusionFilterController(isStrong = false), new ExclusionFilterController(isStrong = true), List(): List[InclusionFilterController])
      .getAllOtherMeasures("202101", "202101", rcuTreatment = ds).cache

    result.collect().foreach(println)

    Assert.assertEquals(1, result.filter(_.pdr.equals("pdr")).count())
    Assert.assertEquals(0, result.filter(_.pdr.equals("pdr2")).count())
    Assert.assertEquals(0, result.filter(_.pdr.equals("pdr3")).count())
    Assert.assertEquals(1, result.filter(_.pdr.equals("pdr4")).count())
  }

  def testRemoveDuplicates(): Unit = {
    Environment.setProperty("flow.read.ghigliottina", "20220909")
    Environment.setProperty("filter.exclusion.enabled", "false")
    Environment.setProperty("filter.strongExclusion.enabled", "false")
    Environment.setProperty("filter.inclusion.pdr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr.enabled", "false")
    Environment.setProperty("filter.inclusion.id_distr_piva_udd.enabled", "false")

    Environment.setProperty("filter.duplicateMeasures.enable", "true")

    val result = new FlowControllerDuplicatesMock(new ExclusionFilterController(isStrong = false), new ExclusionFilterController(isStrong = true), List(): List[InclusionFilterController])
      .getTreatmentMeasures("202101", "202101", getTreatment = false).cache

    val resultIgmg = new FlowControllerDuplicatesMock(new ExclusionFilterController(isStrong = false), new ExclusionFilterController(isStrong = true), List(): List[InclusionFilterController])
      .getIm1IgmgMeasures("202101", "202101", getTreatment = false).cache

    result.collect().foreach(println)
    resultIgmg.collect().foreach(println)

    Assert.assertEquals(0, result.filter(_.pdr.equals("pdr")).count())
    Assert.assertEquals(0, result.filter(_.pdr.equals("pdr1")).count())
    Assert.assertEquals(1, result.filter(_.pdr.equals("pdr2")).count())
    Assert.assertEquals(1, result.filter(_.pdr.equals("pdr3")).count())
    Assert.assertEquals(0, resultIgmg.filter(_.pdr.equals("pdr4")).count())
  }

  class FlowControllerDuplicatesMock(
                                      private val exclusionFilterController: ExclusionFilterController,
                                      private val strongExclusionController: ExclusionFilterController,
                                      private val inclusionFilters: List[InclusionFilterController]
                                    ) extends FlowController(exclusionFilterController, strongExclusionController, inclusionFilters) {
    override val treatmentFlowDAOList: List[MeasureDAO] = List(
      new TglDAODuplMock,
      new A01DAODuplMock,
      new RglDAODuplMock,
      new RmlDAODuplMock
    )

    override val igmgList: List[MeasureDAO] = List(
      new IgmgDAODuplMock
    )

    // Case 1: flows are grouped by pdr, service, date and localFile, but they have different values in measure field. Result => 0 flows
    class TglDAODuplMock extends TglDAO {
      override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
        Environment.getSpark.sparkContext.parallelize(List(
          Tgl(service = "TGL", pdr = "pdr", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None,
            serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")),
          Tgl(service = "TGL", pdr = "pdr", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(1.0), converted = Some(2.0),
            serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva2"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")),
          Tgl(service = "TGL", pdr = "pdr", readType = None, date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(3.0), converted = None,
            serialNumberMis = None, pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = None, isValid = None, dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"))))
      }
    }
    
    // Case 1: they are grouped by pdr, service, date, localFile (there are two groups, 1-2 and 3-4): all the groups have the same fields. Result => 4 flows
    // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked: the filenames are different. Result => 0 flows
    class A01DAODuplMock extends A01DAO {
      override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
        Environment.getSpark.sparkContext.parallelize(List(
          A01(service = "A01", pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("01/01/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S')),
          A01(service = "A01", pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("02/02/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"), readType = Some('S')),
          A01(service = "A01", pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("03/03/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S')),
          A01(service = "A01", pdr = "pdr1", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("04/04/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"), readType = Some('S'))))
      }
    }

    // Case 1: they are grouped by pdr, service, date, localFile (there are two groups, 1-2 and 3-4): the first group is discarded since motivation is different. Result => 2 flows
    // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Result => 2 flows
    // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Result => 2 flows
    // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 1 flow (the last one)
    class RglDAODuplMock extends RglDAO {
      override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
        Environment.getSpark.sparkContext.parallelize(List(
          Rgl(service = "RGL", pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("01/01/2021")), motivation = Some(5), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")),
          Rgl(service = "RGL", pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva1"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("02/02/2021")), motivation = Some(4), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml")),
          Rgl(service = "RGL", pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("03/03/2021")), motivation = Some(3), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml")),
          Rgl(service = "RGL", pdr = "pdr2", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva3"), serialNumberConv = Some("DEF"), dataCaricamento = Some(formatter.parseDateTime("04/04/2021")), motivation = Some(3), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_1/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"))))
      }
    }

    // Case 1: they are grouped by pdr, service, date, localFile (there are three groups, 1-2, 3-4, 5): the first group is discarded since motivation is different. Result => 3 flows
    // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Result => 3 flows
    // Case 3: the remaining flows are grouped by fileName, and localFiles are checked. Since they are different, each flow with inconsistent pivaUtente is discarded. Result => 2 flows
    // Case 4: the remaining flows are grouped by localFile, and everything but dCaricamento is checked. Then the most recent flow is chosen. Result => 1 flow (the chosen flow is random, since dCaricamento is None)
    class RmlDAODuplMock extends RmlDAO {
      override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
        Environment.getSpark.sparkContext.parallelize(List(
          Rml(service = "RML", pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, motivation = Some(5), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
            freqLet = None, tipoRettifica= None, readType = None),
          Rml(service = "RML", pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, motivation = Some(4), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_TGL0050_20200109151501_1.xml"),
            freqLet = None, tipoRettifica = None, readType = None),
          Rml(service = "RML", pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, motivation = Some(3), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva4/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
            freqLet = None, tipoRettifica = None, readType = None),
          Rml(service = "RML", pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, motivation = Some(3), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva4/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
            freqLet = None, tipoRettifica = None, readType = None),
          Rml(service = "RML", pdr = "pdr3", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
            pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, motivation = Some(3), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_piva5/2020/0110/0_1_201912_TGL0050_20200109151501_2.xml"),
            freqLet = None, tipoRettifica = None, readType = None)))
      }
    }

    // Case 1: they are grouped by pdr, service, date, localFile (there are four groups, 1-2, 3-4, 5, 6). In the first group cau_int_cor is different, in the second group there are different measures. Result => 2 flows (5, 6)
    // Case 2: the remaining flows are grouped by timestampLocalFile and fileName is checked. Flows 5 and 6 have the same timeStamp but different fileName. Result => 0 flows
    class IgmgDAODuplMock extends IgmgDAO {
      override def get(startDate: String, endDate: String, getTreatment: Boolean): RDD[Flow] = {
        Environment.getSpark.sparkContext.parallelize(List(
          Igmg(service = "IGMG", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), pivaDistr = None, pivaUtente = Some("piva4"), dataCaricamento = Some(formatter.parseDateTime("01/01/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
            readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), pre = igmgPre1, post = igmgPost1),
          Igmg(service = "IGMG", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), pivaDistr = None, pivaUtente = Some("piva4"), dataCaricamento = Some(formatter.parseDateTime("02/02/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
            readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), pre = igmgPre2, post = igmgPost2),
          Igmg(service = "IGMG", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), pivaDistr = None, pivaUtente = Some("piva4"), dataCaricamento = Some(formatter.parseDateTime("03/03/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151502_1.xml"),
            readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), pre = igmgPre3, post = igmgPost3),
          Igmg(service = "IGMG", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), pivaDistr = None, pivaUtente = Some("piva4"), dataCaricamento = Some(formatter.parseDateTime("04/04/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151502_1.xml"),
            readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), pre = igmgPre4, post = igmgPost4),
          Igmg(service = "IGMG", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), pivaDistr = None, pivaUtente = Some("piva4"), dataCaricamento = Some(formatter.parseDateTime("05/05/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151503_1.xml"),
            readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), pre = igmgPre5, post = igmgPost5),
          Igmg(service = "IGMG", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), pivaDistr = None, pivaUtente = Some("piva4"), dataCaricamento = Some(formatter.parseDateTime("06/06/2021")), localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151503_2.xml"),
            readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), pre = igmgPre6, post = igmgPost6)
        ))
      }
      val igmgPre1: IgmgPre = IgmgPre(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(1), coefCorr = Some(0.0))
      val igmgPost1: IgmgPost = IgmgPost(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPre2: IgmgPre = IgmgPre(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPost2: IgmgPost = IgmgPost(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151501_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPre3: IgmgPre = IgmgPre(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151502_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPost3: IgmgPost = IgmgPost(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151502_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPre4: IgmgPre = IgmgPre(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(1.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151502_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPost4: IgmgPost = IgmgPost(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151502_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPre5: IgmgPre = IgmgPre(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151503_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPost5: IgmgPost = IgmgPost(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151503_1.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPre6: IgmgPre = IgmgPre(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151503_2.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
      val igmgPost6: IgmgPost = IgmgPost(service = "IGMGPRE", pdr = "pdr4", date = Some(formatter.parseDateTime("01/01/2021")), measure = Some(2.0), converted = None, serialNumberMis = Some("ABC"),
        pivaDistr = None, pivaUtente = Some("piva4"), serialNumberConv = Some("DEF"), dataCaricamento = None, localFile = Some("/mnt/isilonshare1/GAS_INJ/TMG_0/DISTRIBUTORE/TMG_0_999/2020/0110/0_1_201912_IGMG0050_20200109151503_2.xml"),
        readType = None, cau_int_mis = Some(0), cau_int_cor = Some(0), coefCorr = Some(0.0))
    }
  }

}
