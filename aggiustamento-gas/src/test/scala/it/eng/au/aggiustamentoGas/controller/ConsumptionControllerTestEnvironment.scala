package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.EnvironmentSparkTest
import it.eng.au.aggiustamentoGas.controller.classeGdM.ClassiGruppiDiMisuraPortataRcugas
import it.eng.au.aggiustamentoGas.dao.rcugas.{RcuGasConnessioniDistr2DAO, RcuGasVarPrelAnnuoPDAO, RcuGasVarProfiloPDAO}
import it.eng.au.aggiustamentoGas.model.agg.{ErrorEnum, ExternalDailyInfo, FlowWithInfo, MonthTreatment}
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg._
import it.eng.au.aggiustamentoGas.model.rcugas._
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasConnessioniDistr2Schema, RcuGasVarPrelAnnuoPSchema, RcuGasVarProfiloPSchema}
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants._
import it.eng.au.aggiustamentoGas.utility.constants.{DimensionalType, Treatment}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class ConsumptionControllerTestEnvironment extends EnvironmentSparkTest {
 /* val getNormalizationCoefficient: (DateTime, String, Int, Int, Map[(String, String, Int), Double]) => Double = (endDate, codProfStandard, idZonaClim, daysNr, pprofMap) => {
    (0 to daysNr).toList.map((_, endDate))
      .map({ case (days, date) => date.minusDays(days).toString("yyyyMMdd") })
      .map(pprofMap(_, codProfStandard, idZonaClim))
      .sum
  }

  def testComputeConsumptionForPdrWithoutMeasures(): Unit = {
    val date = DateTime.parse("15/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val pprofMap = Map(
      (date.minusDays(5), "C2X1", 1) -> 1.5,
      (date.minusDays(4), "C2X1", 1) -> 1.4,
      (date.minusDays(3), "C2X1", 1) -> 1.3,
      (date.minusDays(2), "C2X1", 1) -> 1.2,
      (date.minusDays(1), "C2X1", 1) -> 1.1,
      (date, "C2X1", 1) -> 1.0
    ).map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })

    val pdrValue = "1"
    val extInfo = ExternalDailyInfo(rcuGasSospensioniList = Some(Iterable(RcuGasSuspendedPdr(nIdPdr = "a", dataIniSosp = date, dataFineSosp = date))))

    val emptySegmentRDD = Environment.getSpark.sparkContext.parallelize(List(
      (pdrValue, (List[(FlowWithInfo, FlowWithInfo)](), extInfo))
    ))
    val bcMap = Environment.getSpark.sparkContext.broadcast(pprofMap)

    val classiGdMRangeSterilize: Broadcast[Map[String, Int]] = Environment.getSpark.sparkContext.broadcast(Map())

    val consumptionController = new ConsumptionController
    val consumptionsRDD = consumptionController.calcDailyConsumptions(emptySegmentRDD, bcMap, classiGdMRangeSterilize, "202101", "202101")
    val consumptionList = consumptionsRDD.filter({ case (pdr, (consList, extInfo)) => pdr.equals(pdrValue) })
      .map({ case (pdr, (consList, extInfo)) => consList }).first()

    Assert.assertEquals(31, consumptionList.size)
    consumptionList.foreach(c => Assert.assertNotEquals(ErrorEnum.NO_ERROR_CODE, ErrorEnum.getMaxPriorityError(c.errorCode)))
    //checking suspension precedence over other errors
    Assert.assertTrue(consumptionList.find(_.date.equals(date)).get.isPdrSuspended)
  }

  class RcuGasVarProfiloPDAOMock extends RcuGasVarProfiloPDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("", null, null, "C2X1")
      ).toDF(
        RcuGasVarProfiloPSchema.n_id_pdr,
        RcuGasVarProfiloPSchema.d_data_inizio,
        RcuGasVarProfiloPSchema.d_data_fine,
        RcuGasVarProfiloPSchema.t_cod_profilo
      )
    }
  }

  class RcuGasConnessioniDistr2DAOMock extends RcuGasConnessioniDistr2DAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("", "", "", null, null,  null, null, "1")
      ).toDF(
        RcuGasConnessioniDistr2Schema.t_codice_pdr,
        RcuGasConnessioniDistr2Schema.n_id_distr,
        RcuGasConnessioniDistr2Schema.t_remi,
        RcuGasConnessioniDistr2Schema.d_data_inizio_conn,
        RcuGasConnessioniDistr2Schema.d_data_fine_conn,
        RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione,
        RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione,
        RcuGasConnessioniDistr2Schema.id_regione_climatica
      )
    }
  }

  class RcuGasVarPrelAnnuoPDAOMock extends RcuGasVarPrelAnnuoPDAO {
    override def readParquet: DataFrame = {
      val sqlContext = Environment.getSpark.sqlContext
      import sqlContext.implicits._

      List(
        ("", null, null, "10")
      ).toDF(
        RcuGasVarPrelAnnuoPSchema.n_id_pdr,
        RcuGasVarPrelAnnuoPSchema.d_data_inizio,
        RcuGasVarPrelAnnuoPSchema.d_data_fine,
        RcuGasVarPrelAnnuoPSchema.n_prelievo_annuo
      )
    }
  }

  def testComputeConsumptionForPdrWithoutMeasures2(): Unit = {

    Environment.setProperty("period.read.startDate", "202201")
    Environment.setProperty("period.read.endDate", "202201")

    val format = DateTimeFormat.forPattern("dd/MM/yyyy")
    val date = DateTime.parse("15/02/2022", format)
    val pprofMap = (1 until 65).toList.map(day => {
      (date.minusDays(day), "C2X1", 1) -> day.toDouble / 10.0
    }).toMap
      .map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })

    val pdrValue = "1"
    val rcuGasConnessioniDistr2List = Some((new RcuGasConnessioniDistr2DAOMock).get("202111", "202201").collect().toIterable)
    val rcuGasVarProfiloList = Some((new RcuGasVarProfiloPDAOMock).get("202111", "202201").collect().toIterable)
    val rcuGasVarPrelAnnuoList = Some((new RcuGasVarPrelAnnuoPDAOMock).get("202111", "202201").collect().toIterable)

    val extInfo = ExternalDailyInfo(
      rcuGasConnessioniDistr2List = rcuGasConnessioniDistr2List,
      rcuGasVarProfiloList = rcuGasVarProfiloList,
      rcuGasVarPrelAnnuoList = rcuGasVarPrelAnnuoList
    )

    val emptySegmentRDD = Environment.getSpark.sparkContext.parallelize(List(
      (pdrValue, (List[(FlowWithInfo, FlowWithInfo)](), extInfo))
    ))
    val bcMap = Environment.getSpark.sparkContext.broadcast(pprofMap)
    val classiGdMRangeSterilize: Broadcast[Map[String, Int]] = Environment.getSpark.sparkContext.broadcast(Map())

    val consumptionController = new ConsumptionController
    val consumptionsRDD = consumptionController.calcDailyConsumptions(emptySegmentRDD, bcMap, classiGdMRangeSterilize).cache()

    val cons = consumptionsRDD.map({ case (pdr, (consList, extInfo)) => consList }).first()

    println(cons.mkString("\n"))

    Assert.assertEquals(31, cons.size)
    cons
      .foreach(c => Assert.assertTrue(c.value.isDefined))
  }

  def testComputeConsumptionIgmgF2(): Unit = {

    val format = DateTimeFormat.forPattern("dd/MM/yyyy")
    val date = DateTime.parse("15/02/2022", format)
    val pprofMap = (1 until 65).toList.map(day => {
      (date.minusDays(day), "C2X1", 1) -> day.toDouble / 10.0
    }).toMap
      .map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })

    val pdrValue = "1"
    val rcuGasConnessioniDistr2List = Some((new RcuGasConnessioniDistr2DAOMock).get("202111", "202201").collect().toIterable)
    val rcuGasVarProfiloList = Some((new RcuGasVarProfiloPDAOMock).get("202111", "202201").collect().toIterable)
    val rcuGasVarPrelAnnuoList = Some((new RcuGasVarPrelAnnuoPDAOMock).get("202111", "202201").collect().toIterable)

    val extInfo = ExternalDailyInfo(
      rcuGasConnessioniDistr2List = rcuGasConnessioniDistr2List,
      rcuGasVarProfiloList = rcuGasVarProfiloList,
      rcuGasVarPrelAnnuoList = rcuGasVarPrelAnnuoList
    )

    val dateTgl =  Option(DateTime.parse("15/12/2021", format))

    val mTreatmentTgl = MonthTreatment(pdr = "1", month = "202112", treatment = Treatment.M.toString, calcmode = "", autofilled = false)

    val dummyFlowWithInfoIgmg = FlowWithInfo(
      flow = Tgl(service = "TGL", pdr = "1", date = dateTgl, readType = None, measure = Some(2.0), isValid = None,
        converted = Some(1.0), serialNumberMis = Option("a1"), serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
        dataCaricamento = None),
      dimensionalType = Some(DimensionalType.PK),
      monthTreatment = Some(mTreatmentTgl)
    )

    val dateIgmg =  Option(DateTime.parse("18/01/2022", format))
    val mTreatmentIgmg = MonthTreatment(pdr = "1", month = "202201", treatment = Treatment.M.toString, calcmode = "", autofilled = false)

    val Im1WithInfoAndSameDayMeasureIgmg = FlowWithInfo(
      flow = Igmg(service = "IGMG", pdr = "1", date = dateIgmg, readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = dateIgmg, readType = None, measure = Some(4.0),
          converted = Some(0.90), serialNumberMis = Option("a1"), serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = IgmgPost(service = "IGMGPRE", pdr = "1", date = dateIgmg, readType = None, measure = Some(4.0),
          converted = Some(41.0), serialNumberMis = Option("a1"), serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        sameDayFlow = Some(Tgl(service = "TGL", pdr = "1", date = dateIgmg, readType = None, measure = Some(2.0), isValid = None,
          converted = Some(1.0), serialNumberMis = Option("a1"), serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None))
      ),
      dimensionalType = Some(DimensionalType.PK),
      monthTreatment = Some(mTreatmentIgmg)
    )

    val emptySegmentRDD = Environment.getSpark.sparkContext.parallelize(List(
      (pdrValue, (List[(FlowWithInfo, FlowWithInfo)]((dummyFlowWithInfoIgmg, Im1WithInfoAndSameDayMeasureIgmg)), extInfo))
    ))
    val bcMap = Environment.getSpark.sparkContext.broadcast(pprofMap)
    val classiGdMRangeSterilize: Broadcast[Map[String, Int]] = Environment.getSpark.sparkContext.broadcast(Map())

    val consumptionController = new ConsumptionController
    val consumptionsRDD = consumptionController.calcDailyConsumptions(emptySegmentRDD, bcMap, classiGdMRangeSterilize, "202201", "202201").cache()

    val cons = consumptionsRDD.map({ case (pdr, (consList, extInfo)) => consList }).first()

    println(cons.mkString("\n"))

    val x = 1
    //    Assert.assertEquals(31, cons.size)
    //    cons
    //      .foreach(c => Assert.assertTrue(c.value.isDefined))
  }

  def testComputeConsumptionForPdrWithoutTreatment(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val date = DateTime.parse("15/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val pprofMap = Environment.getSpark.sparkContext.broadcast(Map(
      (date.minusDays(5), "C2X1", 1) -> 1.5,
      (date.minusDays(4), "C2X1", 1) -> 1.4,
      (date.minusDays(3), "C2X1", 1) -> 1.3,
      (date.minusDays(2), "C2X1", 1) -> 1.2,
      (date.minusDays(1), "C2X1", 1) -> 1.1,
      (date, "C2X1", 1) -> 1.0
    ).map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) }))

    val leftTgl = Tgl(service = "TGL", pdr = "1", date = Some(date.minusDays(5)), readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val rightTgl = Tgl(service = "TGL", pdr = "1", date = Some(date), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val rcuGasValPrelAnnuo = RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = Some(0.1), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuGasVarProfilo = RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))


    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1), dataFineConn = date.plusYears(1), nIdDistr = "", tRemi = "", idRegioneClimatica = Some(1))

    val segmentList = List[(FlowWithInfo, FlowWithInfo)](
      (FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = None),
        FlowWithInfo(flow = rightTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = None))
    )
    val extInfo = ExternalDailyInfo(
      rcuGasMassivoPList = None,
      rcuGasConnessioniDistr2List = Some(Iterable(conn2Distr)),
      rcuGasVarProfiloList = Some(Iterable(rcuGasVarProfilo)),
      rcuGasVarPrelAnnuoList = Some(Iterable(rcuGasValPrelAnnuo))
    )

    val noTreatmentSegmentRDD = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentList, extInfo))
    ))
    val classiGdMRangeSterilize: Broadcast[Map[String, Int]] = Environment.getSpark.sparkContext.broadcast(Map())

    val consumptionController = new ConsumptionController()
    val consumptionsRDD = consumptionController.calcDailyConsumptions(noTreatmentSegmentRDD, pprofMap, classiGdMRangeSterilize)
    val consumptionList = consumptionsRDD.filter({ case (pdr, (consList, extInfo)) => pdr.equals("1") })
      .map({ case (pdr, (consList, extInfo)) => consList }).first()

    consumptionList.map(_.date).foreach(println)

    Assert.assertEquals(31, consumptionList.size)
    consumptionList.foreach(c => {
      c.date match {
        case x: DateTime if x.equals(date) =>
          Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_NULL_ERROR_CODE))
          Assert.assertEquals(0.1 * 1.0, c.value.get, 0.0)
        case x: DateTime if x.equals(date.minusDays(1)) =>
          Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_NULL_ERROR_CODE))
          Assert.assertEquals(0.1 * 1.1, c.value.get, 0.0)
        case x: DateTime if x.equals(date.minusDays(2)) =>
          Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_NULL_ERROR_CODE))
          Assert.assertEquals(0.1 * 1.2, c.value.get, 0.0)
        case x: DateTime if x.equals(date.minusDays(3)) =>
          Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_NULL_ERROR_CODE))
          Assert.assertEquals(0.1 * 1.3, c.value.get, 0.0)
        case x: DateTime if x.equals(date.minusDays(4)) =>
          Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_NULL_ERROR_CODE))
          Assert.assertEquals(0.1 * 1.4, c.value.get, 0.0)
        case _ =>
      }
    })

  }

  def testComputeConsumptions(): Unit = { //MONTHTREATMENT MUST BE VALUED
    val consumptionController = new ConsumptionController

    val date = DateTime.parse("15/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))
    val pprofMap = Map(
      (date.minusDays(5), "C2X1", 1) -> 1.5,
      (date.minusDays(4), "C2X1", 1) -> 1.4,
      (date.minusDays(3), "C2X1", 1) -> 1.3,
      (date.minusDays(2), "C2X1", 1) -> 1.2,
      (date.minusDays(1), "C2X1", 1) -> 1.1,
      (date, "C2X1", 1) -> 1.0
    ).map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })

    val leftTgl = Tgl(service = "TGL", pdr = "1", date = Some(date), readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val rightTgl = Tgl(service = "TGL", pdr = "1", date = Some(date), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1),
      dataFineConn = date.plusYears(1), idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")

    val YMonthTreatment = MonthTreatment(treatment = Treatment.Y.toString, pdr = null, month = null, calcmode = null, autofilled = false)
    val NMonthTreatment = MonthTreatment(treatment = Treatment.N.toString, pdr = null, month = null, calcmode = null, autofilled = false)
    val GMonthTreatment = MonthTreatment(treatment = Treatment.G.toString, pdr = null, month = null, calcmode = null, autofilled = false)
    val rcuMassivoActiveFurniture = RcuGasMassivoP(startDate = date.minusYears(50), endDate = date.plusYears(50), tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)

    // Scenario 1: same month, one day of difference, both have treatment G
    val consumptionsList1 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(date.plusDays(1))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)), rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture)))
    )
    Assert.assertEquals(1, consumptionsList1.size)
    Assert.assertEquals(date.plusDays(1), consumptionsList1.head.date)
    Assert.assertEquals(G_FORMULA_ID, consumptionsList1.head.idFormula)

    // Scenario 2: dx is not at first day of month, more than one day of difference, both have treatment G

    val consumptionsList2 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl.copy(date = Some(date.minusDays(5))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      FlowWithInfo(flow = rightTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)), rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture)))
    )
    Assert.assertEquals(5, consumptionsList2.size)
    consumptionsList2.foreach(c => {
      Assert.assertEquals(NULL_CONSUMPTION_FORMULA_ID, c.idFormula)
      Assert.assertEquals(ErrorEnum.COD_PROF_STD_ERROR_CODE, ErrorEnum.getMaxPriorityError(c.errorCode))
      Assert.assertEquals(0.0, c.value.get, 0.0)
    })

    // Scenario 3: different month, one day of difference, dx has treatment G
    val consumptionsList3 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl.copy(date = Some(DateTime.parse("31/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(YMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(DateTime.parse("01/02/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )
    Assert.assertEquals(1, consumptionsList3.size)
    Assert.assertEquals(DateTime.parse("01/02/2021", DateTimeFormat.forPattern("dd/MM/yyyy")), consumptionsList3.head.date)
    Assert.assertEquals(G_FORMULA_ID, consumptionsList3.head.idFormula)

    // Scenario 4: dx and sx are G and between them there is at least a month with treatment N (i.e. a month with no measures)
    val sxDate = leftTgl.date.get
    val dxDate = date.plusMonths(2)

    val consumptionsList4 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(dxDate)), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )
    val daysNr = DateUtility.daysBetween(sxDate, dxDate)
    val datesSet = (0 until daysNr).map(offset => dxDate.minusDays(offset).withTimeAtStartOfDay).toSet
    val consDatesSet = consumptionsList4.map(_.date).toSet

    Assert.assertEquals(datesSet.size, datesSet.intersect(consDatesSet).size)
    Assert.assertEquals(Set(), datesSet -- consDatesSet)
    Assert.assertEquals(daysNr, consumptionsList4.size)

    //Scenario 5 treatment is N on both sides
    val consumptionsList5 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(NMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(date.plusDays(10))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(NMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )

    Assert.assertEquals(10, consumptionsList5.size)
    consumptionsList5.foreach(c => {
      Assert.assertTrue(c.value.isEmpty)
      Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_N_ERROR_CODE))
    })

    //Scenario 6 sx has treatment N and dx doesn't
    val consumptionsList6 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(NMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(date.plusMonths(1))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(YMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )

    val datesSet6 = (0 until DateUtility.daysBetween(date, date.plusMonths(1)))
      .map(offset => date.plusMonths(1).minusDays(offset)).toSet
    val consDatesSet6 = consumptionsList6.map(_.date).toSet
    Assert.assertEquals(datesSet6.size, consDatesSet6.size)
    Assert.assertEquals(Set(), datesSet6 -- consDatesSet6)
    Assert.assertEquals(31, consumptionsList6.size)
    consumptionsList6.foreach(c => {
      if (c.date.monthOfYear.equals(date.monthOfYear)) {
        Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_N_ERROR_CODE))
      } else {
        Assert.assertFalse(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_N_ERROR_CODE))
      }
    })

    //Scenario 7 dx has treatment N and sx doesn't
    val consumptionsList7 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(YMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(date.plusMonths(1))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(NMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )

    val datesSet7 = (0 until DateUtility.daysBetween(date, date.plusMonths(1)))
      .map(offset => date.plusMonths(1).minusDays(offset)).toSet

    val consDatesSet7 = consumptionsList7.map(_.date).toSet
    Assert.assertEquals(datesSet7.size, consDatesSet7.size)
    Assert.assertEquals(Set(), datesSet7 -- consDatesSet7)

    Assert.assertEquals(31, consumptionsList7.size)
    consumptionsList7.foreach(c => {
      if (c.date.monthOfYear.equals(date.monthOfYear)) {
        Assert.assertFalse(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_N_ERROR_CODE))
      } else {
        Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_N_ERROR_CODE))
      }
    })

    // Scenario 8: both dx and sx are G, they are not in consecutive days, they are in consecutive months
    val consumptionsList8 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(date.plusMonths(1))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )

    val datesSet8 = (0 until DateUtility.daysBetween(date, date.plusMonths(1)))
      .map(offset => date.plusMonths(1).minusDays(offset)).toSet

    val consDatesSet8 = consumptionsList8.map(_.date).toSet
    Assert.assertEquals(datesSet8.size, consDatesSet8.size)
    Assert.assertEquals(Set(), datesSet8 -- consDatesSet8)

    Assert.assertEquals(31, consumptionsList8.size)
    consumptionsList8.foreach(c => {
      Assert.assertEquals(NULL_CONSUMPTION_FORMULA_ID, c.idFormula)

      Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.NOT_CONSECUTIVE_DAYS_ERROR_CODE) ||
        c.errorCode.toSet.contains(ErrorEnum.COD_PROF_STD_ERROR_CODE) ||
        c.errorCode.toSet.contains(ErrorEnum.ID_REG_CLIM_ERROR_CODE) ||
        c.errorCode.toSet.contains(ErrorEnum.PPROF_K_ERROR_CODE))
    })

    // Scenario 9: dx is at first day of month, more than one month of difference, both have treatment G
    val consumptionsList9 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl.copy(date = Some(DateTime.parse("15/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(DateTime.parse("01/03/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )
    Assert.assertEquals(45, consumptionsList9.size)
    consumptionsList9.foreach(c => {
      if (c.date.isBefore(DateTime.parse("01/03/2021", DateTimeFormat.forPattern("dd/MM/yyyy"))) &&
        c.date.isAfter(DateTime.parse("31/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))) {
        Assert.assertEquals(NULL_CONSUMPTION_FORMULA_ID, c.idFormula)
        Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_N_ERROR_CODE))
        Assert.assertTrue(c.value.isEmpty)
      } else {
        Assert.assertEquals(M_Y_FORMULA_ID, c.idFormula)
        Assert.assertNotEquals(ErrorEnum.NO_ERROR_CODE, ErrorEnum.getMaxPriorityError(c.errorCode))
      }
    })

    // Scenario 10: dx is not at first day of month, more than one month of difference, both have treatment G
    val consumptionsList10 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTgl.copy(date = Some(DateTime.parse("15/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      FlowWithInfo(flow = rightTgl.copy(date = Some(DateTime.parse("02/03/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))), dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null, monthTreatment = Some(GMonthTreatment)),
      pprofMap,
      ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )
    Assert.assertEquals(46, consumptionsList10.size)
    consumptionsList10.foreach(c => {
      Assert.assertEquals(NULL_CONSUMPTION_FORMULA_ID, c.idFormula)
      Assert.assertTrue(c.errorCode.toSet.contains(ErrorEnum.NOT_CONSECUTIVE_DAYS_ERROR_CODE))
      Assert.assertEquals(0.0, c.value.get, 0.0)
    })

  }

  def testCalcDailyConsumptions(): Unit = {
    val computationStartDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime(Environment.getPeriodStartDate).dayOfMonth().withMinimumValue()
    val computationEndDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime(Environment.getPeriodEndDate).dayOfMonth().withMaximumValue()
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val daysNr = DateUtility.daysBetween(computationStartDate, computationEndDate)
    val datesList = (0 to daysNr).toList.map(daysOffset => computationStartDate.plusDays(daysOffset))

    val pprofMap = datesList.map(date => ((date, "C2X1", 1), 2.0)).toMap.map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })
    val tabProfiliGiorniStdPercBCMap: Broadcast[Map[(String, String, Int), Double]] = Environment.getSpark.sparkContext.broadcast(pprofMap)

    val tml = Tml(service = "TML", pdr = "1", date = None, readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)
    val YMonthTreatment = MonthTreatment(treatment = Treatment.Y.toString, pdr = null, month = null, calcmode = null, autofilled = false)

    val measuresWithInfos = datesList.map(date => tml.copy(date = Some(date)))
      .map(flow => FlowWithInfo(flow = flow, dimensionalType = Some(DimensionalType.C), monthTreatment = Some(YMonthTreatment)))
    val segmentsWithInfos = measuresWithInfos.zip(measuresWithInfos.tail)

    val rcuGasValPrelAnnuo = RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = Some(0.1), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuGasVarProfilo = RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuMassivoActiveFurniture = RcuGasMassivoP(startDate = computationStartDate.minusYears(50), endDate = computationStartDate.plusYears(50), tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)


    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = computationStartDate,
      dataFineConn = computationEndDate, idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")

    val extInfos = ExternalDailyInfo(
      rcuGasConnessioniDistr2List = Some(List(conn2Distr)),
      rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)),
      rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)),
      rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture)))

    val segmentsRDD: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentsWithInfos, extInfos))
    ))
    val classiGdMRangeSterilize: Broadcast[Map[String, Int]] = Environment.getSpark.sparkContext.broadcast(Map())

    val consumptionController = new ConsumptionController()
    val consumptionRdd = consumptionController.calcDailyConsumptions(segmentsRDD, tabProfiliGiorniStdPercBCMap, classiGdMRangeSterilize)

    val consumptions = consumptionRdd.filter(_._1.equals("1")).values.first()._1
    Assert.assertEquals(31, consumptions.size)
    consumptions.foreach(consumption => {
      Assert.assertTrue(consumption.value.isDefined || consumption.date.equals(computationStartDate))
    })

    // Head-profiling scenario [HPS]
    val segmentsRDDHPS: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentsWithInfos.tail.tail.tail, extInfos))
    ))
    val consumptionRddHPS = consumptionController.calcDailyConsumptions(segmentsRDDHPS, tabProfiliGiorniStdPercBCMap, classiGdMRangeSterilize)
    val consumptionsHPS = consumptionRddHPS.filter(_._1.equals("1")).values.first()._1

    consumptionsHPS.map(_.date).foreach(println)

    Assert.assertEquals(31, consumptionsHPS.size)

    consumptionsHPS.foreach(consumption => Assert.assertTrue(datesList.toSet.contains(consumption.date)))

  }

  def testApplyCAFormula(): Unit = {
    val consumptionController = new ConsumptionController

    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val computationDate = formatter.parseDateTime("13-05-2021")
    val pprofMap = Map(
      (computationDate.minusDays(5), "C2X1", 1) -> 1.5,
      (computationDate.minusDays(4), "C2X1", 1) -> 1.4,
      (computationDate.minusDays(3), "C2X1", 1) -> 1.3,
      (computationDate.minusDays(2), "C2X1", 1) -> 1.2,
      (computationDate.minusDays(1), "C2X1", 1) -> 1.1,
      (computationDate, "C2X1", 1) -> 1.0
    ).map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })
    val measureDate = computationDate.minusDays(5)

    val measure = Tml(service = "TML", pdr = "1", date = Some(measureDate), readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = measureDate.minusYears(1),
      dataFineConn = measureDate.plusYears(1), idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")

    val rcuGasValPrelAnnuo = List(
      RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = Some(1.0), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    )
    val rcuGasVarProfilo = List(
      RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    )

    val consumptionsList = consumptionController.applyCAFormula(
      pdr = "1",
      startDate = measure.date.get,
      pprofMap = pprofMap,
      forwardFlag = true,
      computationDate = computationDate,
      externalDailyInfo = ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)), rcuGasVarPrelAnnuoList = Some(rcuGasValPrelAnnuo), rcuGasVarProfiloList = Some(rcuGasVarProfilo))
    )
    consumptionsList.foreach(println)
    Assert.assertEquals(5, consumptionsList.size)
    consumptionsList.foreach(consumption => {
      consumption.date match {
        case d: DateTime if d.equals(computationDate) => Assert.assertEquals(1.0 * 1.0, consumption.value.get, 0.0)
        case d: DateTime if d.equals(computationDate.minusDays(1)) => Assert.assertEquals(1.1 * 1.0, consumption.value.get, 0.0)
        case d: DateTime if d.equals(computationDate.minusDays(2)) => Assert.assertEquals(1.2 * 1.0, consumption.value.get, 0.0)
        case d: DateTime if d.equals(computationDate.minusDays(3)) => Assert.assertEquals(1.3 * 1.0, consumption.value.get, 0.0)
        case d: DateTime if d.equals(computationDate.minusDays(4)) => Assert.assertEquals(1.4 * 1.0, consumption.value.get, 0.0)
        case d: DateTime if d.equals(computationDate.minusDays(5)) => Assert.assertEquals(1.5 * 1.0, consumption.value.get, 0.0)
        case _ => Assert.assertTrue(false)
      }
    })

    val nullConsumptionsList = consumptionController.applyCAFormula(
      pdr = "1",
      startDate = measure.date.get,
      pprofMap = pprofMap,
      forwardFlag = false,
      computationDate = computationDate,
      externalDailyInfo = ExternalDailyInfo(rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )
    Assert.assertEquals(5, nullConsumptionsList.size)
    nullConsumptionsList.foreach(consumption => {
      Assert.assertNotEquals(ErrorEnum.NO_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
    })
  }

  def testApplyMYFormula(): Unit = {
    val consumptionController = new ConsumptionController

    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val date = formatter.parseDateTime("01-12-2020")
    val pprofMap = Map(
      (date.minusDays(5), "C2X1", 1) -> 1.5,
      (date.minusDays(4), "C2X1", 1) -> 1.4,
      (date.minusDays(3), "C2X1", 1) -> 1.3,
      (date.minusDays(2), "C2X1", 1) -> 1.2,
      (date.minusDays(1), "C2X1", 1) -> 1.1,
      (date, "C2X1", 1) -> 1.0
    ).map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })
    val leftTml = Tml(service = "TML", pdr = "1", date = Some(date.minusDays(5)), readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val rightTml = Tml(service = "TML", pdr = "1", date = Some(date), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val rcuGasValPrelAnnuo = RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = None, dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuGasVarProfilo = RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))

    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1),
      dataFineConn = date.plusYears(1), idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")

    val consumptionsList = consumptionController.applyMYFormula(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)), rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)), rcuGasConnessioniDistr2List = Some(List(conn2Distr))),
      gcFlag = false,
      misMatch = false
    )
    val normCoeff = getNormalizationCoefficient(date, "C2X1", 1, 5, pprofMap)
    Assert.assertEquals(5, consumptionsList.size)
    consumptionsList.foreach(consumption => {
      consumption.date match {
        case d: DateTime if d.equals(date) => Assert.assertEquals(1.0 * 18.0 / normCoeff, consumption.value.get, 0.0)
        case d: DateTime if d.equals(date.minusDays(1)) => Assert.assertEquals(1.1 * 18.0 / normCoeff, consumption.value.get, 0.0)
        case d: DateTime if d.equals(date.minusDays(2)) => Assert.assertEquals(1.2 * 18.0 / normCoeff, consumption.value.get, 0.0)
        case d: DateTime if d.equals(date.minusDays(3)) => Assert.assertEquals(1.3 * 18.0 / normCoeff, consumption.value.get, 0.0)
        case d: DateTime if d.equals(date.minusDays(4)) => Assert.assertEquals(1.4 * 18.0 / normCoeff, consumption.value.get, 0.0)
        case d: DateTime if d.equals(date.minusDays(5)) => Assert.assertEquals(1.5 * 18.0 / normCoeff, consumption.value.get, 0.0)
        case _ => Assert.assertTrue(false) //should never happen
      }
    })

    //Scenarios with errors
    val rcuMassivoActiveFurniture = RcuGasMassivoP(startDate = date.minusYears(50), endDate = date.plusYears(50), tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
    val nullConsumptionsList1 = consumptionController.applyMYFormula(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(
        rcuGasVarProfiloList = Some(List(rcuGasVarProfilo.copy(dataInizio = date.plusYears(50)))),
        rcuGasConnessioniDistr2List = Some(List(conn2Distr)),
        rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture))
      ),
      gcFlag = false,
      misMatch = false
    )
    Assert.assertEquals(5, nullConsumptionsList1.size)
    nullConsumptionsList1.foreach(consumption => {
      Assert.assertTrue(consumption.value.isEmpty)
      Assert.assertEquals(ErrorEnum.COD_PROF_STD_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
    })

    val nullConsumptionsList2 = consumptionController.applyMYFormula(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(
        rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)),
        rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)),
        rcuGasConnessioniDistr2List = Some(List(conn2Distr.copy(dataInizioConn = date.plusDays(500), dataFineConn = date.plusDays(500)))),
        rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture))
      ),
      gcFlag = false,
      misMatch = false
    )
    Assert.assertEquals(5, nullConsumptionsList2.size)
    nullConsumptionsList2.foreach(consumption => {
      Assert.assertTrue(consumption.value.isEmpty)
      Assert.assertEquals(ErrorEnum.ID_REG_CLIM_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
    })

    val nullConsumptionsList3 = consumptionController.applyMYFormula(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(
        rcuGasVarProfiloList = Some(List(rcuGasVarProfilo.copy(tCodProfilo = None))),
        rcuGasConnessioniDistr2List = Some(List(conn2Distr)),
        rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture))
      ),
      gcFlag = false,
      misMatch = false
    )
    Assert.assertEquals(5, nullConsumptionsList3.size)
    nullConsumptionsList3.foreach(consumption => {
      Assert.assertTrue(consumption.value.isEmpty)
      Assert.assertEquals(ErrorEnum.COD_PROF_STD_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
    })

    val nullConsumptionsList4 = consumptionController.applyMYFormula(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(
        rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)),
        rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)),
        rcuGasConnessioniDistr2List = Some(List(conn2Distr.copy(idRegioneClimatica = None))),
        rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture))
      ),
      gcFlag = false,
      misMatch = false
    )
    Assert.assertEquals(5, nullConsumptionsList4.size)
    nullConsumptionsList4.foreach(consumption => {
      Assert.assertTrue(consumption.value.isEmpty)
      Assert.assertEquals(ErrorEnum.ID_REG_CLIM_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
    })

    //best effort scenario: computing for all available pprofk
    val bestEffortList = consumptionController.applyMYFormula(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      Map(pprofMap.head._1 -> pprofMap.head._2),
      ExternalDailyInfo(
        rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)),
        rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)),
        rcuGasConnessioniDistr2List = Some(List(conn2Distr)),
        rcuGasMassivoPList = Some(List(
          RcuGasMassivoP(startDate = date.minusYears(10), endDate = date.plusYears(10), tCodicePdr = "", nIdPdr = "",
            tTrattamento = Treatment.G, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)
        ))
      ),
      gcFlag = false,
      misMatch = false
    )
    Assert.assertEquals(5, bestEffortList.size)
    bestEffortList.foreach(consumption => {
      if (consumption.value.isDefined) {
        Assert.assertEquals(ErrorEnum.NO_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
        Assert.assertEquals(pprofMap.head._1._1, consumption.date.toString("yyyyMMdd"))
      } else {
        Assert.assertTrue(consumption.value.isEmpty)
        Assert.assertEquals(ErrorEnum.PPROF_K_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
      }
    })
  }

  def testComputeDifferenceOnlyConsumptionWithSerialCheck(): Unit = {
    val consumptionController = new ConsumptionController

    val leftTgl = Tgl(service = "TGL", pdr = "1", date = Some(DateTime.now()), readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val rightTgl = Tgl(service = "TGL", pdr = "1", date = Some(DateTime.now()), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    //serial Number does not match and the measure are not pre/post instances
    val emptyConsumption = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      FlowWithInfo(flow = leftTgl.copy(serialNumberMis = Some("c")), dimensionalType = Some(DimensionalType.P)),
      FlowWithInfo(flow = rightTgl, dimensionalType = Some(DimensionalType.P))
    )
    Assert.assertEquals(None, emptyConsumption.value)

    // Apply G formula using converted field
    val consumptionC = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C)),
      FlowWithInfo(flow = rightTgl, dimensionalType = Some(DimensionalType.C))
    )
    Assert.assertEquals(FlowWithInfo(flow = leftTgl, dimensionalType = Some(DimensionalType.C)), consumptionC.startMeasure)
    Assert.assertEquals(FlowWithInfo(flow = rightTgl, dimensionalType = Some(DimensionalType.C)), consumptionC.endMeasure)
    Assert.assertTrue(consumptionC.pprof.isEmpty)
    Assert.assertEquals(consumptionC.date.withTimeAtStartOfDay(), leftTgl.date.get.withTimeAtStartOfDay())
    Assert.assertEquals(G_FORMULA_ID, consumptionC.idFormula)
    Assert.assertEquals(18.0, consumptionC.value.get, 0.0)

    // Apply G formula using measure*coefficient
    val consumptionPK = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      FlowWithInfo(flow = leftTgl, coeff = Some(3.0), dimensionalType = Some(DimensionalType.PK)),
      FlowWithInfo(flow = rightTgl, coeff = Some(2.0), dimensionalType = Some(DimensionalType.PK))
    )
    Assert.assertTrue(consumptionPK.pprof.isEmpty)
    Assert.assertEquals(consumptionPK.date.withTimeAtStartOfDay(), leftTgl.date.get.withTimeAtStartOfDay())
    Assert.assertEquals(G_FORMULA_ID, consumptionPK.idFormula)
    Assert.assertEquals((rightTgl.measure.get - leftTgl.measure.get) * 2.0, consumptionPK.value.get, 0.0)

    // Apply G formula using measure*(im1/igmg coefficient)
    val consumptionPKIm1 = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      FlowWithInfo(flow = leftTgl, coeff = Some(3.0), dimensionalType = Some(DimensionalType.PK)),
      FlowWithInfo(flow = rightTgl, coeff = Some(2.0), dimensionalType = Some(DimensionalType.PK), im1IgmgCoeff = Some(500.0))
    )
    Assert.assertTrue(consumptionPKIm1.pprof.isEmpty)
    Assert.assertEquals(consumptionPKIm1.date.withTimeAtStartOfDay(), leftTgl.date.get.withTimeAtStartOfDay())
    Assert.assertEquals(G_FORMULA_ID, consumptionPKIm1.idFormula)
    Assert.assertEquals((rightTgl.measure.get - leftTgl.measure.get) * 500.0, consumptionPKIm1.value.get, 0.0)

    // Apply G formula using measure filled
    val consumptionP = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      FlowWithInfo(flow = leftTgl, coeff = Some(3.0), dimensionalType = Some(DimensionalType.P)),
      FlowWithInfo(flow = rightTgl, coeff = Some(2.0), dimensionalType = Some(DimensionalType.P))
    )
    Assert.assertTrue(consumptionP.pprof.isEmpty)
    Assert.assertEquals(consumptionP.date.withTimeAtStartOfDay(), leftTgl.date.get.withTimeAtStartOfDay())
    Assert.assertEquals(G_FORMULA_ID, consumptionP.idFormula)
    Assert.assertEquals(rightTgl.measure.get - leftTgl.measure.get, consumptionP.value.get, 0.0)


    //verify that for im1/igmg pre/post serial number check is ignored and the consumption is computed anyway
    val igmgPreWithWrongSerials = IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(DateTime.now()), readType = None, measure = Some(11.0),
      converted = Some(0.0), serialNumberMis = Some("notMatchingMis"), serialNumberConv = Some("notMatchingConv"), coefCorr = Some(5.0), cau_int_mis = None,
      cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)

    val consumptionPKIGMG = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      FlowWithInfo(flow = leftTgl, coeff = Some(3.0), dimensionalType = Some(DimensionalType.PK)),
      FlowWithInfo(flow = igmgPreWithWrongSerials, coeff = Some(2.0), dimensionalType = Some(DimensionalType.PK))
    )
    Assert.assertTrue(consumptionPKIGMG.pprof.isEmpty)
    Assert.assertEquals(consumptionPKIGMG.date.withTimeAtStartOfDay(), leftTgl.date.get.withTimeAtStartOfDay())
    Assert.assertEquals(G_FORMULA_ID, consumptionPKIGMG.idFormula)
    //    Assert.assertEquals((igmgPreWithWrongSerials.measure.get - leftTgl.measure.get) * igmgPreWithWrongSerials.coefCorr.get, consumptionPKIGMG.value.get, 0.0)

  }


  def testCreateZeroValuedConsumptionsInTheInterval(): Unit = {
    val consumptionController = new ConsumptionController

    val date = Some(DateTime.parse("1/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))
    val leftTgl = Tgl(service = "TGL", pdr = "1", date = date, readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val rightTgl = Tgl(service = "TGL", pdr = "1", date = Some(date.get.plusMonths(1)), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val consumptionList = consumptionController.createZeroValuedConsumptionsInTheInterval(FlowWithInfo(flow = leftTgl), FlowWithInfo(flow = rightTgl))

    consumptionList.foreach(consumption => {
      Assert.assertEquals(Some(0.0), consumption.value)
      Assert.assertEquals(NULL_CONSUMPTION_FORMULA_ID, consumption.idFormula)
      Assert.assertEquals(ErrorEnum.NOT_CONSECUTIVE_DAYS_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
    })

    Assert.assertEquals(31, consumptionList.size)
    Assert.assertFalse(consumptionList.exists(c => c.date.withTimeAtStartOfDay().isEqual(date.get.withTimeAtStartOfDay())))
    Assert.assertTrue(consumptionList.exists(c => c.date.withTimeAtStartOfDay().isEqual(rightTgl.date.get.withTimeAtStartOfDay())))

    consumptionList.map(_.date).foreach(println)
  }

  def testCreateNullConsumptionBetween(): Unit = {
    val consumptionController = new ConsumptionController

    val date = Some(DateTime.parse("1/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))
    val leftTgl = Tgl(service = "TGL", pdr = "1", date = date, readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val rightTgl = Tgl(service = "TGL", pdr = "1", date = Some(date.get.plusMonths(1)), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val consumptionList = consumptionController.createNullConsumptionBetween(leftTgl.date.get, rightTgl.date.get, FlowWithInfo(flow = leftTgl), FlowWithInfo(flow = rightTgl))

    consumptionList.foreach(consumption => {
      Assert.assertTrue(consumption.value.isEmpty)
      Assert.assertEquals(NULL_CONSUMPTION_FORMULA_ID, consumption.idFormula)
      Assert.assertEquals(ErrorEnum.TREATMENT_IS_N_ERROR_CODE, ErrorEnum.getMaxPriorityError(consumption.errorCode))
    })

    Assert.assertFalse(consumptionList.exists(c => c.date.withTimeAtStartOfDay().isEqual(date.get.withTimeAtStartOfDay())))
    Assert.assertTrue(consumptionList.exists(c => c.date.withTimeAtStartOfDay().isEqual(rightTgl.date.get.withTimeAtStartOfDay())))

    consumptionList.map(_.date).foreach(println)
  }

  def testAreSerialNumbersCoerent(): Unit = {
    val consumptionController = new ConsumptionController

    val dummyTgl = Tgl(service = "tgl", pdr = "1", date = None, readType = None, measure = None, isValid = None,
      converted = None, serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val PK = Some(DimensionalType.PK)
    val P = Some(DimensionalType.P)
    val C = Some(DimensionalType.C)


    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl), FlowWithInfo(flow = dummyTgl))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s1")), dimensionalType = PK), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s1")), dimensionalType = PK))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s1")), dimensionalType = C), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s2")), dimensionalType = C))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl, dimensionalType = PK), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s2")), dimensionalType = P))
    )

    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberConv = Some("s1")), dimensionalType = C), FlowWithInfo(flow = dummyTgl.copy(serialNumberConv = Some("s1")), dimensionalType = C))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberConv = Some("s1")), dimensionalType = C), FlowWithInfo(flow = dummyTgl.copy(serialNumberConv = Some("s2")), dimensionalType = PK))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl, dimensionalType = C), FlowWithInfo(flow = dummyTgl.copy(serialNumberConv = Some("s2")), dimensionalType = C))
    )

    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s11"), serialNumberConv = Some("s1")), dimensionalType = C), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s11"), serialNumberConv = Some("s1")), dimensionalType = C))
    )
    Assert.assertFalse(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s15"), serialNumberConv = Some("s1")), dimensionalType = P), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s11"), serialNumberConv = Some("s1")), dimensionalType = P))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = None, serialNumberConv = Some("s1")), dimensionalType = C), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s11"), serialNumberConv = Some("s1")), dimensionalType = C))
    )

    Assert.assertFalse(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberConv = Some("s1")), dimensionalType = C), FlowWithInfo(flow = dummyTgl.copy(serialNumberConv = Some("s2")), dimensionalType = C))
    )
    Assert.assertFalse(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s1")), dimensionalType = P), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("s2")), dimensionalType = P))
    )

    val impre = Im1Pre(service = "IM1PRE", pdr = "1", date = None, readType = None, measure = None,
      converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)
    val impost = Im1Post(service = "IM1PRE", pdr = "1", date = None, readType = None, measure = None,
      converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)
    val igmgpre = IgmgPre(service = "IGMGPRE", pdr = "1", date = None, readType = None, measure = None,
      converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)
    val igmgpost = IgmgPost(service = "IGMGPRE", pdr = "1", date = None, readType = None, measure = None,
      converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)

    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl), FlowWithInfo(flow = impre))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl), FlowWithInfo(flow = igmgpre))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("c1"))), FlowWithInfo(flow = impre.copy(serialNumberMis = Some("c2"))))
    )
    //    Assert.assertFalse(
    //      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("c1"))), FlowWithInfo(flow = igmgpre.copy(serialNumberMis = Some("c2"))))
    //    )

    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = impost), FlowWithInfo(flow = dummyTgl))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = igmgpost), FlowWithInfo(flow = dummyTgl))
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = impost.copy(serialNumberMis = Some("c1"))), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("c2"))))
    )
    //    Assert.assertFalse(
    //      consumptionController.areSerialNumbersCoherent(FlowWithInfo(flow = igmgpost.copy(serialNumberMis = Some("c1"))), FlowWithInfo(flow = dummyTgl.copy(serialNumberMis = Some("c2"))))
    //    )

    val tgl = Tgl(service = "tgl", pdr = "1", date = None, readType = None, measure = None, isValid = None,
      converted = None, serialNumberMis = Some("123"), serialNumberConv = Some("123"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None)

    val igmg = Igmg(service = "IGMGPRE", pdr = "1", date = None, readType = None, measure = None,
      converted = None, serialNumberMis = Some("123"), serialNumberConv = None, coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
      pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = None, readType = None, measure = None,
        converted = None, serialNumberMis = Some("123"), serialNumberConv = Some("124"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
      sameDayFlow = Some(Tgl(service = "tgl", pdr = "1", date = None, readType = None, measure = None, isValid = None,
        converted = None, serialNumberMis = Some("124"), serialNumberConv = Some("124"), pivaDistr = None, pivaUtente = None, localFile = None,
        dataCaricamento = None)),
      post = IgmgPost(service = "IGMGPOST", pdr = "1", date = None, readType = None, measure = None,
        converted = None, serialNumberMis = Some("124"), serialNumberConv = Some("124"), coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)
    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(dimensionalType = P, flow = tgl), FlowWithInfo(dimensionalType = P, flow = igmg))
    )
    //    Assert.assertFalse(
    //      consumptionController.areSerialNumbersCoherent(FlowWithInfo(dimensionalType = C, flow = tgl), FlowWithInfo(dimensionalType = C, flow = igmg))
    //    )
    Assert.assertTrue(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(dimensionalType = C, flow = tgl), FlowWithInfo(dimensionalType = C, flow = igmg))
    )

    val igmgPre =  IgmgPre(service = "IGMGPRE", pdr = "1", date = None, readType = None, measure = None,
      converted = None, serialNumberMis = Some("999"), serialNumberConv = Some("999"), coefCorr = None, cau_int_mis = None,
      cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)

    Assert.assertFalse(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(dimensionalType = P, flow = tgl), FlowWithInfo(dimensionalType = P, flow = igmgPre))
    )
    Assert.assertFalse(
      consumptionController.areSerialNumbersCoherent(FlowWithInfo(dimensionalType = C, flow = tgl), FlowWithInfo(dimensionalType = C, flow = igmgPre))
    )
  }

  def testIm1IgmgPrePostSplit(): Unit = {
    val consumptionController = new ConsumptionController

    val date = Some(DateTime.parse("1/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))
    val monthTreatment = Some(MonthTreatment(pdr = "", treatment = Treatment.G.toString, month = "", calcmode = "", autofilled = true))
    val Im1WithInfo = FlowWithInfo(
      flow = Im1(service = "IM1", pdr = "1", date = date, readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = Im1Pre(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = Im1Post(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )

    val dummyFlowWithInfo = FlowWithInfo(
      flow = Tgl(service = "tgl", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = Some(2.0), isValid = None,
        converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
        dataCaricamento = None),
      dimensionalType = Some(DimensionalType.PK),
      monthTreatment = monthTreatment
    )

    //case im1 is start Tgl is post, return a couple (Post, Tgl) with dimType = PK because converted is not defined
    val (newStartMeasure, newEndMeasure) = consumptionController.splitIm1Igmg2PrePost(List((Im1WithInfo, dummyFlowWithInfo)), 0).head
    Assert.assertTrue(newStartMeasure.flow.isInstanceOf[Im1Post])
    Assert.assertTrue(newEndMeasure.flow.isInstanceOf[Tgl])

    //case Tgl is start im1 is end, return a couple (Tgl, Pre) with dimType = PK because converted is not defined
    val (newStartMeasure1, newEndMeasure1) = consumptionController.splitIm1Igmg2PrePost(List((dummyFlowWithInfo, Im1WithInfo)), 0).head
    Assert.assertTrue(newStartMeasure1.flow.isInstanceOf[Tgl])
    Assert.assertTrue(newEndMeasure1.flow.isInstanceOf[Im1Pre])

    val Im1WithInfoAndSameDayMeasure = FlowWithInfo(
      flow = Im1(service = "IM1", pdr = "1", date = dummyFlowWithInfo.flow.date.map(_.plusDays(1)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = Im1Pre(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = Im1Post(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        sameDayFlow = Some(Rgl(service = "RGL", pdr = "1", date = date.map(_.plusDays(1)), measure = Some(2.0),
          converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None, motivation = None))
      ),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )
    val dummyFlowWithInfoConsecutiveToIm = dummyFlowWithInfo.copy(flow = dummyFlowWithInfo.flow.asInstanceOf[Tgl].copy(date = Im1WithInfoAndSameDayMeasure.flow.date.map(_.plusDays(1))))

    val correction = Rgl(service = "RGL", pdr = "10", date = date.map(_.plusDays(1)),
      measure = Some(2), converted = Some(10), serialNumberMis = None, pivaDistr = Some("1"), pivaUtente = None, serialNumberConv = None,
      localFile = Some("00489490011_12420101003_201912_TGL0050_20200109151501_2.xml"),
      motivation = Some(4), dataCaricamento = None)
    val igmgWithPostCorrectedByMot4 = FlowWithInfo(
      flow = Igmg(service = "IGMG", pdr = "10", date = dummyFlowWithInfoConsecutiveToIm.flow.date.map(_.plusDays(1)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = IgmgPre(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = IgmgPost(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = None,
          converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
          correctionFlow = Some(correction), isCorrected = true),
        sameDayFlow = Some(Rgl(service = "RGL", pdr = "1", date = date.map(_.plusDays(1)), measure = Some(2.0),
          converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None, motivation = None))
      ),
      dimensionalType = Some(DimensionalType.P),
      monthTreatment = monthTreatment
    )


    val measureList = List(dummyFlowWithInfo, Im1WithInfo, dummyFlowWithInfo, Im1WithInfoAndSameDayMeasure, dummyFlowWithInfoConsecutiveToIm, igmgWithPostCorrectedByMot4)
    val segments = measureList.zip(measureList.tail)

    val results = consumptionController.splitIm1Igmg2PrePost(segments, 0)

    Assert.assertTrue(results.head._1.flow.isInstanceOf[Tgl])
    Assert.assertTrue(results.head._2.flow.isInstanceOf[Pre])

    Assert.assertTrue(results(1)._1.flow.isInstanceOf[Post])
    Assert.assertTrue(results(1)._2.flow.isInstanceOf[Tgl])

    Assert.assertTrue(results(2)._1.flow.isInstanceOf[Tgl])
    Assert.assertTrue(results(2)._2.flow.isInstanceOf[Im1Igmg])

    Assert.assertTrue(results(3)._1.flow.isInstanceOf[Rgl])
    Assert.assertTrue(results(3)._2.flow.isInstanceOf[Tgl])

    Assert.assertTrue(results(4)._2.flow.isInstanceOf[Im1Igmg])
    Assert.assertTrue(results(4)._2.flow.asInstanceOf[Im1Igmg].sameDayFlow.get.isInstanceOf[Post])

    val measureListStartingWithIm1 = List(Im1WithInfo, dummyFlowWithInfo, Im1WithInfoAndSameDayMeasure, dummyFlowWithInfoConsecutiveToIm)
    val segmentsStartingWithIm1 = measureListStartingWithIm1.zip(measureListStartingWithIm1.tail)

    val resultsPrime = consumptionController.splitIm1Igmg2PrePost(segmentsStartingWithIm1, 0)
    Assert.assertTrue(resultsPrime.head._1.flow.isInstanceOf[Post])
    Assert.assertTrue(resultsPrime.head._2.flow.isInstanceOf[Tgl])

  }

  def testIm1IgmgFormula4Forcing(): Unit = {
    val consumptionController = new ConsumptionController

    val date = Some(DateTime.parse("1/01/2021", DateTimeFormat.forPattern("dd/MM/yyyy")))

    val dummyFlowWithInfo = FlowWithInfo(
      flow = Tgl(service = "TGL", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = Some(2.0), isValid = None,
        converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
        dataCaricamento = None),
      dimensionalType = Some(DimensionalType.PK)
    )

    val dummyFlowWithInfoRGL = FlowWithInfo(
      flow = Rgl(service = "RGL", pdr = "1", date = date.map(_.plusDays(1)), measure = Some(2.0), converted = Some(1.0),
        serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        motivation = None),
      dimensionalType = Some(DimensionalType.PK)
    )

    val Im1WithInfoAndSameDayMeasure = FlowWithInfo(
      flow = Im1(service = "IM1", pdr = "1", date = dummyFlowWithInfo.flow.date.map(_.plusDays(1)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = Im1Pre(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = Some(4.0),
          converted = Some(0.90), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = Im1Post(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = Some(4.0),
          converted = Some(41.0), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        sameDayFlow = Some(Rgl(service = "RGL", pdr = "1", date = date.map(_.plusDays(1)), measure = Some(2.0),
          converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None, motivation = None))
      ),
      dimensionalType = Some(DimensionalType.P)
    )

    val Im1WithInfoAndSameDayMeasureWithPostCorrected = FlowWithInfo(
      flow = Im1(service = "IM1", pdr = "1", date = dummyFlowWithInfo.flow.date.map(_.plusDays(1)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = Im1Pre(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = Some(4.0),
          converted = Some(0.90), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = Im1Post(service = "IM1PRE", pdr = "1", date = date, readType = None, measure = Some(4.0),
          converted = Some(41.0), serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None, isCorrected = true),
        sameDayFlow = Some(Rgl(service = "RGL", pdr = "1", date = date.map(_.plusDays(1)), measure = Some(2.0),
          converted = Some(1.0), serialNumberMis = None, serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None, motivation = None))
      ),
      dimensionalType = Some(DimensionalType.P)
    )
    val k = CoefficientController.COEFFICIENT_DEFAULT
    val c = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(dummyFlowWithInfo, Im1WithInfoAndSameDayMeasure)

    Assert.assertEquals(
      (Im1WithInfoAndSameDayMeasure.flow.asInstanceOf[Im1].pre.converted.get - dummyFlowWithInfo.flow.converted.get) +
        (Im1WithInfoAndSameDayMeasure.flow.asInstanceOf[Im1].sameDayFlow.get.measure.get - Im1WithInfoAndSameDayMeasure.flow.asInstanceOf[Im1].post.measure.get) * k,
      c.value.get,
      0.0001
    )

    val c2 = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(dummyFlowWithInfoRGL, Im1WithInfoAndSameDayMeasureWithPostCorrected)
    Assert.assertEquals(
      (Im1WithInfoAndSameDayMeasureWithPostCorrected.flow.asInstanceOf[Im1].pre.converted.get - dummyFlowWithInfoRGL.flow.converted.get) +
        (Im1WithInfoAndSameDayMeasureWithPostCorrected.flow.asInstanceOf[Im1].sameDayFlow.get.converted.get - Im1WithInfoAndSameDayMeasureWithPostCorrected.flow.asInstanceOf[Im1].post.converted.get),
      c2.value.get,
      0.0001
    )


    val dummyFlowWithInfoIgmg = FlowWithInfo(
      flow = Tgl(service = "TGL", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = Some(2.0), isValid = None,
        converted = Some(1.0), serialNumberMis = Option("a1"), serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
        dataCaricamento = None),
      dimensionalType = Some(DimensionalType.PK)
    )

    val Im1WithInfoAndSameDayMeasureIgmg = FlowWithInfo(
      flow = Igmg(service = "IGMG", pdr = "1", date = dummyFlowWithInfoIgmg.flow.date.map(_.plusDays(1)), readType = None, measure = None,
        converted = None, serialNumberMis = None, serialNumberConv = None, coefCorr = None, cau_int_mis = None,
        cau_int_cor = Some(3), pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
        pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = date, readType = None, measure = Some(4.0),
          converted = Some(0.90), serialNumberMis = Option("a1"), serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        post = IgmgPost(service = "IGMGPRE", pdr = "1", date = date, readType = None, measure = Some(4.0),
          converted = Some(41.0), serialNumberMis = Option("a1"), serialNumberConv = None, coefCorr = None, cau_int_mis = None,
          cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
        sameDayFlow = Some(Tgl(service = "TGL", pdr = "1", date = date.map(_.plusDays(1)), readType = None, measure = Some(2.0), isValid = None,
          converted = Some(1.0), serialNumberMis = Option("a1"), serialNumberConv = None, pivaDistr = None, pivaUtente = None, localFile = None,
          dataCaricamento = None))
      ),
      dimensionalType = Some(DimensionalType.PK)
    )

    val c3 = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(dummyFlowWithInfoIgmg, Im1WithInfoAndSameDayMeasureIgmg)

    Assert.assertFalse(c3.errorCode.contains(ErrorEnum.NON_MATCHING_SERIALS_ERROR_CODE))

    val c4 = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      dummyFlowWithInfoIgmg.copy(flow = dummyFlowWithInfoIgmg.flow.asInstanceOf[Tgl].copy(serialNumberMis = Option("b1")))
      , Im1WithInfoAndSameDayMeasureIgmg
    )
    Assert.assertTrue(c4.errorCode.contains(ErrorEnum.NON_MATCHING_SERIALS_ERROR_CODE))

    val c5 = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      dummyFlowWithInfoIgmg
      , Im1WithInfoAndSameDayMeasureIgmg
        .copy(flow = Im1WithInfoAndSameDayMeasureIgmg.flow.asInstanceOf[Igmg]
          .copy(sameDayFlow = Some(Im1WithInfoAndSameDayMeasureIgmg.flow.asInstanceOf[Igmg].sameDayFlow.get.asInstanceOf[Tgl]
            .copy(serialNumberMis = Option("b1"))
          )
          )
        )
    )

    Assert.assertFalse(c5.errorCode.contains(ErrorEnum.NON_MATCHING_SERIALS_ERROR_CODE))

    val c6 = consumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
      dummyFlowWithInfoIgmg
      , Im1WithInfoAndSameDayMeasureIgmg
        .copy(flow = Im1WithInfoAndSameDayMeasureIgmg.flow.asInstanceOf[Igmg]
          .copy(sameDayFlow = Some(Im1WithInfoAndSameDayMeasureIgmg.flow.asInstanceOf[Igmg].sameDayFlow.get.asInstanceOf[Tgl]
            .copy(serialNumberMis = Option("b1"), converted = None)
          )
          )
        )
    )

    Assert.assertTrue(c6.errorCode.contains(ErrorEnum.NON_MATCHING_SERIALS_ERROR_CODE))
  }

  def testEmptyFlowListSegmentCreation(): Unit = {
    val pdrNoMeasureRDD = Environment.getSpark.sparkContext.parallelize(List(
      ("pdr", (List[FlowWithInfo](), ExternalDailyInfo()))
    ))
    val segments = new ConsumptionController().calcCouple(pdrNoMeasureRDD)
    Assert.assertTrue(segments.filter({ case (k, v) => k.equals("pdr") }).first()._2._1.isEmpty)
  }

  def testCheckActivation(): Unit = {
    val computationStartDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime(Environment.getPeriodStartDate).dayOfMonth().withMinimumValue()
    val computationEndDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime(Environment.getPeriodEndDate).dayOfMonth().withMaximumValue()
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val daysNr = DateUtility.daysBetween(computationStartDate, computationEndDate)
    val datesList = (0 to daysNr).toList.map(daysOffset => computationStartDate.plusDays(daysOffset))

    val pprofMap = datesList.map(date => ((date, "C2X1", 1), 2.0)).toMap.map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })
    val tabProfiliGiorniStdPercBCMap: Broadcast[Map[(String, String, Int), Double]] = Environment.getSpark.sparkContext.broadcast(pprofMap)

    val a40 = A40(service = "A40", pdr = "1", date = Some(datesList.head), readType = None, measure = Some(1.0),
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, outcome = None)
    val tml = Tml(service = "TML", pdr = "1", date = None, readType = None, measure = Some(2.0), isValid = None,
      converted = Some(3.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val YMonthTreatment = MonthTreatment(treatment = Treatment.Y.toString, pdr = null, month = null, calcmode = null, autofilled = false)

    val measuresWithInfos = datesList.map(date => tml.copy(date = Some(date))).map(_.setActivationFlow(Option(a40)))
      .map(flow => FlowWithInfo(flow = flow, dimensionalType = Some(DimensionalType.C), monthTreatment = Some(YMonthTreatment)))

    val segmentsWithInfos = measuresWithInfos.zip(measuresWithInfos.tail)

    val rcuGasValPrelAnnuo = RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = Some(0.1), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuGasVarProfilo = RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuMassivoActiveFurniture = RcuGasMassivoP(startDate = computationStartDate.minusYears(50), endDate = computationStartDate.plusYears(50), tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)


    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = computationStartDate,
      dataFineConn = computationEndDate, idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")
    val extInfos = ExternalDailyInfo(
      rcuGasConnessioniDistr2List = Some(List(conn2Distr)),
      rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)),
      rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)),
      rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture)))

    val segmentsRDD: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentsWithInfos, extInfos))
    ))
    val classiGdMRangeSterilize: Broadcast[Map[String, Int]] = Environment.getSpark.sparkContext.broadcast(Map())

    val consumptionController = new ConsumptionController()
    val consumptionRdd = consumptionController.calcDailyConsumptions(segmentsRDD, tabProfiliGiorniStdPercBCMap, classiGdMRangeSterilize).cache()

    Assert.assertEquals(consumptionRdd.collect().head._2._1.head.value.get, 0.0, 0)
    Assert.assertTrue(consumptionRdd.collect().head._2._1.head.startMeasure.flow.isInstanceOf[A40])

    val YMonthTreatment2 = MonthTreatment(treatment = Treatment.M.toString, pdr = null, month = null, calcmode = null, autofilled = false)
    val measuresWithInfos2 = datesList.map(date => tml.copy(date = Some(date))).map(_.setActivationFlow(Option(a40)))
      .map(flow => FlowWithInfo(flow = flow, dimensionalType = Some(DimensionalType.C), monthTreatment = Some(YMonthTreatment2)))

    val segmentsWithInfos2 = measuresWithInfos2.zip(measuresWithInfos2.tail)

    val segmentsRDD2: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentsWithInfos2, extInfos))
    ))
    val consumptionRdd2 = consumptionController.calcDailyConsumptions(segmentsRDD2, tabProfiliGiorniStdPercBCMap, classiGdMRangeSterilize).cache()

    Assert.assertEquals(consumptionRdd2.collect().head._2._1.head.value.get, 0.0, 0)
    Assert.assertTrue(consumptionRdd2.collect().head._2._1.head.startMeasure.flow.isInstanceOf[A40])

    val YMonthTreatment3 = MonthTreatment(treatment = Treatment.G.toString, pdr = null, month = null, calcmode = null, autofilled = false)
    val measuresWithInfos3 = datesList.map(date => tml.copy(date = Some(date))).map(_.setActivationFlow(Option(a40)))
      .map(flow => FlowWithInfo(flow = flow, dimensionalType = Some(DimensionalType.C), monthTreatment = Some(YMonthTreatment3)))

    val segmentsWithInfos3 = measuresWithInfos3.zip(measuresWithInfos3.tail)

    val segmentsRDD3: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentsWithInfos3, extInfos))
    ))
    val consumptionRdd3 = consumptionController.calcDailyConsumptions(segmentsRDD3, tabProfiliGiorniStdPercBCMap, classiGdMRangeSterilize).cache()
    consumptionRdd3.collect().foreach(println)

    Assert.assertEquals(consumptionRdd3.collect().head._2._1.head.value.get, 1.0, 0)
    Assert.assertTrue(consumptionRdd3.collect().head._2._1.head.startMeasure.flow.isInstanceOf[A40])

    val measures: List[Flow] = List(a40.copy(date = Option(datesList(2)))) ::: datesList.tail.tail.tail.map(date => tml.copy(date = Some(date)))
    val measuresWithInfos4 = measures
      .map(flow => FlowWithInfo(flow = flow, dimensionalType = Some(DimensionalType.C), monthTreatment = Some(YMonthTreatment.copy(treatment = Treatment.G.toString))))

    val segmentsWithInfos4 = measuresWithInfos4.zip(measuresWithInfos4.tail)

    val segmentsRDD4: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentsWithInfos4, extInfos))
    ))
    segmentsRDD4.collect().foreach(println)
    val consumptionRdd4 = consumptionController.calcDailyConsumptions(segmentsRDD4, tabProfiliGiorniStdPercBCMap, classiGdMRangeSterilize).cache()
    consumptionRdd4.collect().foreach(println)

    val x = 1
  }

  def testSterilized(): Unit = {
    val computationStartDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime("202009").dayOfMonth().withMinimumValue()
    val computationEndDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime("202011").dayOfMonth().withMaximumValue()
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val daysNr = DateUtility.daysBetween(computationStartDate, computationEndDate)
    val datesList = (0 to daysNr).toList.map(daysOffset => computationStartDate.plusDays(daysOffset))

    val pprofMap = datesList.map(date => ((date, "C2X1", 1), 2.0)).toMap.map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })
    val tabProfiliGiorniStdPercBCMap: Broadcast[Map[(String, String, Int), Double]] = Environment.getSpark.sparkContext.broadcast(pprofMap)

    val tmlleft = Tml(service = "TML", pdr = "1", date = Some(formatter.parseDateTime("01-10-2020")), readType = None, measure = Some(2.0), isValid = None,
      converted = Some(3.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)
    val tmlright = Tml(service = "TML", pdr = "1", date = Some(formatter.parseDateTime("01-11-2020")), readType = None, measure = Some(3000.0), isValid = None,
      converted = Some(500000.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val YMonthTreatment = MonthTreatment(treatment = Treatment.Y.toString, pdr = null, month = null, calcmode = null, autofilled = false)

    val segmentsWithInfos = List(
      (FlowWithInfo(flow = tmlleft, dimensionalType = Some(DimensionalType.C), monthTreatment = Some(YMonthTreatment)),
        FlowWithInfo(flow = tmlright, dimensionalType = Some(DimensionalType.C), monthTreatment = Some(YMonthTreatment))
      ))

    val rcuGasValPrelAnnuo = RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = Some(0.1), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuGasVarProfilo = RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuMassivoActiveFurniture = RcuGasMassivoP(startDate = computationStartDate.minusYears(50), endDate = computationStartDate.plusYears(50), tCodicePdr = "", nIdPdr = "", tTrattamento = Treatment.Y, pivaUdd = None, tTipoFornitura = None, tComuneIstatPdr = None, tComuneIstatForn = None)

    val rcuGasTechList = Some(List(
      RcuGasTech(nIdPdr = "1", startDateTech = formatter.parseDateTime("01-10-2020"), endDateTech = formatter.parseDateTime("15-10-2020"), classeMisuratore = Some("G1,6"), nCifreMis = None, nCifreConv = None)
      , RcuGasTech(nIdPdr = "1", startDateTech = formatter.parseDateTime("16-10-2020"), endDateTech = formatter.parseDateTime("01-12-2020"), classeMisuratore = Some("G4"), nCifreMis = None, nCifreConv = None)
    ))

    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = computationStartDate,
      dataFineConn = computationEndDate, idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")
    val extInfos = ExternalDailyInfo(
      rcuGasConnessioniDistr2List = Some(List(conn2Distr)),
      rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)),
      rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)),
      rcuGasMassivoPList = Some(List(rcuMassivoActiveFurniture)),
      rcuGasTechList = rcuGasTechList
    )

    val segmentsRDD: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = Environment.getSpark.sparkContext.parallelize(List(
      ("1", (segmentsWithInfos, extInfos))
    ))
    val classiGdMRangeSterilize: Broadcast[Map[String, Int]] = Environment.getSpark.sparkContext.broadcast((new ClassiGruppiDiMisuraPortataRcugas).get.collectAsMap().toMap)

    val consumptionController = new ConsumptionController()
    val consumptionRdd = consumptionController.calcDailyConsumptions(segmentsRDD, tabProfiliGiorniStdPercBCMap, classiGdMRangeSterilize, "202009", "202011", true).cache()

    println(consumptionRdd.values.collect.head._1.mkString("\n"))

    Assert.assertEquals(144.0, consumptionRdd.values.collect.head._1.filter(_.date.equals(formatter.parseDateTime("20-10-2020"))).head.value.get, 0)
    Assert.assertTrue(consumptionRdd.values.collect.head._1.filter(_.date.equals(formatter.parseDateTime("20-10-2020"))).head.valueNotSterilized.isDefined)
    Assert.assertEquals(60.0, consumptionRdd.values.collect.head._1.filter(_.date.equals(formatter.parseDateTime("10-10-2020"))).head.value.get, 0)
    Assert.assertTrue(consumptionRdd.values.collect.head._1.filter(_.date.equals(formatter.parseDateTime("10-10-2020"))).head.valueNotSterilized.isDefined)

  }

  def testApplyMYFormula2WithTreatmentYanddoesOverwriteValueWithF3IfTreamtmentiIsYForSBG(): Unit = {
    val consumptionController = new ConsumptionController
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val date = formatter.parseDateTime("01-12-2020")
    val pprofMap = Map(
      (date.minusDays(5), "C2X1", 1) -> 1.5,
      (date.minusDays(4), "C2X1", 1) -> 1.4,
      (date.minusDays(3), "C2X1", 1) -> 1.3,
      (date.minusDays(2), "C2X1", 1) -> 1.2,
      (date.minusDays(1), "C2X1", 1) -> 1.1,
      (date, "C2X1", 1) -> 1.0
    ).map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })
    val leftTml = Tml(service = "TML", pdr = "1", date = Some(date.minusDays(5)), readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val rightTml = Tml(service = "TML", pdr = "1", date = Some(date), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val rcuGasValPrelAnnuo = RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = Some(1.0), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuGasVarProfilo = RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))

    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1),
      dataFineConn = date.plusYears(1), idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")

    val consumptionsList = consumptionController.applyMYFormula(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)), rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)), rcuGasConnessioniDistr2List = Some(List(conn2Distr))), gcFlag = false, misMatch = false
    )

    println(consumptionsList.mkString("\n"))

    Assert.assertTrue(consumptionsList.head.valueF3.isDefined)

  }

  def testApplyMYFormula1WithTreatmentYanddoesOverwriteValueWithF3IfTreamtmentiIsYForSBG(): Unit = {
    val consumptionController = new ConsumptionController
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")
    val date = formatter.parseDateTime("01-12-2020")
    val pprofMap = Map(
      (date.minusDays(5), "C2X1", 1) -> 1.5,
      (date.minusDays(4), "C2X1", 1) -> 1.4,
      (date.minusDays(3), "C2X1", 1) -> 1.3,
      (date.minusDays(2), "C2X1", 1) -> 1.2,
      (date.minusDays(1), "C2X1", 1) -> 1.1,
      (date, "C2X1", 1) -> 1.0
    ).map({ case (k, v) => ((k._1.toString("yyyyMMdd"), k._2, k._3), v) })
    val leftTml = Tml(service = "TML", pdr = "1", date = Some(date.minusDays(1)), readType = None, measure = Some(1.0), isValid = None,
      converted = Some(2.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val rightTml = Tml(service = "TML", pdr = "1", date = Some(date), readType = None, measure = Some(10.0), isValid = None,
      converted = Some(20.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
      dataCaricamento = None, coefCorr = None, freqLet = None)

    val rcuGasValPrelAnnuo = RcuGasVarPrelAnnuoP(nIdPdr = "a", nPrelivevoAnnuo = Some(1.0), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))
    val rcuGasVarProfilo = RcuGasVarProfiloP(nIdPdr = "a", tCodProfilo = Some("C2X1"), dataInizio = formatter.parseDateTime("01-10-2020"), dataFine = formatter.parseDateTime("30-09-2021"))

    val conn2Distr = RcuGasConnessioniDistr2(tCodicePdr = "1", dataInizioConn = date.minusYears(1),
      dataFineConn = date.plusYears(1), idRegioneClimatica = Some(1), nIdDistr = "", tRemi = "")

    val consumptionsList = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)), rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)), rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )

    println(consumptionsList.mkString("\n"))
    Assert.assertTrue(consumptionsList.head.valueF3.isDefined)

    val consumptionsList3 = consumptionController.computeConsumptions(
      FlowWithInfo(flow = leftTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      FlowWithInfo(flow = rightTml, dimensionalType = Some(DimensionalType.C), idRegioneClimatica = null),
      pprofMap,
      ExternalDailyInfo(rcuGasVarPrelAnnuoList = Some(List(rcuGasValPrelAnnuo)), rcuGasVarProfiloList = Some(List(rcuGasVarProfilo)), rcuGasConnessioniDistr2List = Some(List(conn2Distr)))
    )

    println("\n")
    println(consumptionsList3.mkString("\n"))
    Assert.assertTrue(consumptionsList3.head.valueF3.isDefined)

  }
  //TODO da rivedere formula 4

  /* def testHybridFormula4(): Unit = {
     val igmg = Igmg(service = "IGMGPRE", pdr = "1", date = Some(DateTime.parse("2021-01-30", DateTimeFormat.forPattern("yyyy-MM-dd"))), readType = None, measure = None,
       converted = None, serialNumberMis = Some("a"), serialNumberConv = Some("b"), coefCorr = None, cau_int_mis = None,
       cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None,
       pre = IgmgPre(service = "IGMGPRE", pdr = "1", date = Some(DateTime.parse("2021-01-30", DateTimeFormat.forPattern("yyyy-MM-dd"))), readType = None,
         measure = Some(4.0),
         converted = Some(5.0),  serialNumberMis = Some("a"), serialNumberConv = Some("b"), coefCorr = None, cau_int_mis = None,
         cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None),
       sameDayFlow = Some(Tml(service = "TML", pdr = "1", date = Some(DateTime.parse("2021-01-30", DateTimeFormat.forPattern("yyyy-MM-dd"))), readType = None,
         measure = Some(6.0), isValid = None,
         converted = Some(7.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
         dataCaricamento = None, coefCorr = None, freqLet = None)),
       post = IgmgPost(service = "IGMGPOST", pdr = "1", date = Some(DateTime.parse("2021-01-30", DateTimeFormat.forPattern("yyyy-MM-dd"))), readType = None,
         measure = Some(1.0),
         converted = Some(2.0),  serialNumberMis = Some("a"), serialNumberConv = Some("b"), coefCorr = None, cau_int_mis = None,
         cau_int_cor = None, pivaDistr = None, pivaUtente = None, localFile = None, dataCaricamento = None)
     )
     val tml = Tml(service = "TML", pdr = "1", date = None, readType = None,
       measure = Some(2.0), isValid = None,
       converted = Some(3.0), serialNumberMis = Some("a"), serialNumberConv = Some("b"), pivaDistr = None, pivaUtente = None, localFile = None,
       dataCaricamento = None, coefCorr = None, freqLet = None)

     val emptyConsumption = ConsumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
       FlowWithInfo(flow = tml, dimensionalType = Some(DimensionalType.C)),
       FlowWithInfo(flow = igmg, dimensionalType = Some(DimensionalType.C))
     )

     Assert.assertEquals(Some(7.0), emptyConsumption.value)

     val emptyConsumption2 = ConsumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
       FlowWithInfo(flow = tml.copy(converted = None), dimensionalType = Some(DimensionalType.H)),
       FlowWithInfo(flow = igmg, dimensionalType = Some(DimensionalType.H))
     )

     Assert.assertEquals(Some(7.0), emptyConsumption2.value)

     val emptyConsumption3 = ConsumptionController.computeDifferenceOnlyConsumptionWithSerialCheck(
       FlowWithInfo(flow = tml, dimensionalType = Some(DimensionalType.H)),
       FlowWithInfo(flow = igmg.copy(post = igmg.post.copy(converted = None)), dimensionalType = Some(DimensionalType.H))
     )

     Assert.assertEquals(Some(7.0), emptyConsumption3.value)

   }*/

  */
}
