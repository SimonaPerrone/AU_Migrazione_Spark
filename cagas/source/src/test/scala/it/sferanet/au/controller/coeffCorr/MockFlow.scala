package it.sferanet.au.controller.coeffCorr

import it.sferanet.au.controller.visitor.{IFlowVisitor, IFlowWithReturnVisitor}
import it.sferanet.au.model.Flow
import it.sferanet.au.model.prestazionale.{IgmgPost, IgmgPre, Im1Post, Im1Pre}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.util.Random

case class MockFlow(service: String = "A01",
                    pdr: String, //cod_pdr
                    date: Option[Date],
                    pivaDistr: Option[String],
                    pivaUtente: Option[String],
                    measure: Option[Double] = Option(1), //Dato Misura 1
                    converted: Option[Double] = Option(100), //Dato Misura 2
                    serialNumberMis: Option[String] = Option("MATMIS"), //matricola misuratore
                    serialNumberConv: Option[String] = Option("MATCONV"), //matricola convertitore
                    local_file: Option[String] = None,
                    d_caricamento: Option[Date] = None,
                    override val ammissibilita: Option[String],
                    override val coefCorr: Option[Double] = None,
                    cau_int_cor: Option[Int] = None,
                    override val isNewRoute: Boolean = true) extends Flow {

  override def accept(visitor: IFlowVisitor): Unit = {

  }

  override def accept[TReturnValue](visitor: IFlowWithReturnVisitor[TReturnValue]): TReturnValue = {
    null.asInstanceOf[TReturnValue]
  }

}

object MockFlow {

  val formatString = "yyyy/MM/dd"
  val formatter = new SimpleDateFormat(formatString)
  val initialDate: String = "2008/01/10" // Start date
  var finalDate: String = "3000/12/31"
  val rand = new Random()

  val flowsServices: List[String] = List("A01", "A40", "RGL", "RML", "RMV", "RSL", "SM1", "SW1", "TAL", "TAS", "TAV", "TGL",
    "TML", "TMV", "R01", "A02", "V01", "M01", "V02", "A01R", "A02R", "A40R ", "AD2", "AD2R", "AD3", "AD3R ", "AD4", "AD4R ",
    "AD5", "AD5R ", "FDD ", "FUI", "M01R ", "R01R ", "R40 ", "R40R ", "S02 ", "S02R ", "S40 ", "S40R ", "V01R ", "V02R")

  def generateNMockFlows(n: Int, pdrs: List[String]): List[MockFlow] = {
    val c: Calendar = Calendar.getInstance()
    c.setTime(formatter.parse(initialDate))
    //    c.add(Calendar.DATE, 1);  // number of days to add
    //    dt = sdf.format(c.getTime());  // dt is now the new date
    val mockList: List[MockFlow] = for (
      pdr <- pdrs;
      i <- 0 to n
    ) yield {
      c.setTime(formatter.parse(initialDate))
      c.add(Calendar.MONTH, i)
      MockFlow(date = Option(c.getTime), pdr = pdr, service = flowsServices(rand.nextInt(flowsServices.length)), pivaDistr = None, pivaUtente = None, ammissibilita = None)
    }
    finalDate = c.getTime.formatted(formatString)
    mockList
  }

  def generateIm1MFlow(pdr: String, date: Date, coeffPre: Double = 1.0, coeffPost: Double = 1.0, cau_int_cor: Option[Int] = None): List[MockFlow] = {
    List(MockFlow(pdr = pdr, service = "IM1PRE", date = Option(date), coefCorr = Option(coeffPre), cau_int_cor = cau_int_cor, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      MockFlow(pdr = pdr, service = "IM1POST", date = Option(date), coefCorr = Option(coeffPost), cau_int_cor = cau_int_cor, pivaDistr = None, pivaUtente = None, ammissibilita = None))
  }

  def generateIgmgMFlow(pdr: String, date: Date, coeffPre: Double = 1.0, coeffPost: Double = 1.0, cau_int_cor: Option[Int] = None): List[MockFlow] = {
    List(MockFlow(pdr = pdr, service = "IGMGPRE", date = Option(date), coefCorr = Option(coeffPre), cau_int_cor = cau_int_cor, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      MockFlow(pdr = pdr, service = "IGMGPOST", date = Option(date), coefCorr = Option(coeffPost), cau_int_cor = cau_int_cor, pivaDistr = None, pivaUtente = None, ammissibilita = None))
  }


  def generateIgmgMockFlow(pdr: String, date: String, coeffPre: Double = 1.0, coeffPost: Double = 1.0, cau_int_cor: Option[Int] = None): List[MockFlow] = {
    generateIgmgMFlow(pdr, formatter.parse(date), coeffPre, coeffPost, cau_int_cor)
  }

  def generateIm1MockFlow(pdr: String, date: String, coeffPre: Double = 1.0, coeffPost: Double = 1.0, cau_int_cor: Option[Int] = None): List[MockFlow] = {
    generateIm1MFlow(pdr, formatter.parse(date), coeffPre, coeffPost, cau_int_cor)
  }

  def generateIm1(pdr: String, date: Date, cau_int_cor: Option[Int] = None, coeffPre: Double = 1.0, coeffPost: Double = 1.0, serialNumberMis: Option[String] = None, serialNumberConv: Option[String] = None, converted: Option[Double] = None): List[Flow] = {
    List(Im1Pre(pdr = pdr, service = "IM1PRE", date = Option(date), cau_int_cor = cau_int_cor, readType = Some('E'),
      measure = Some(1.0), converted = converted, serialNumberMis = serialNumberMis, serialNumberConv = serialNumberConv, cau_int_mis = None,
      coefCorr = Some(coeffPre), local_file = Some("/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12300020158/2019/1107/05608890488_12300020158_201910_RSL0400_20191107143000_1.xml")
      , d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None).asInstanceOf[Flow],
      Im1Post(pdr = pdr, service = "IM1POST", date = Option(date), cau_int_cor = cau_int_cor, readType = Some('E'),
        measure = Some(1.0), converted = converted, serialNumberMis = serialNumberMis, serialNumberConv = serialNumberConv, cau_int_mis = None,
        coefCorr = Some(coeffPost), local_file = Some("/mnt/isilonshare1/GAS_INJ_RECUPERO/TMG_05608890488/DISTRIBUTORE/TMG_05608890488_12300020158/2019/1107/05608890488_12300020158_201910_RSL0400_20191107143000_1.xml")
        , d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None).asInstanceOf[Flow])
  }

  def generateIgmg(pdr: String, date: Date, cau_int_cor: Option[Int] = None, coeffPre: Double = 1.0, coeffPost: Double = 1.0, serialNumberMis: Option[String] = None, serialNumberConv: Option[String] = None, converted: Option[Double] = None): List[Flow] = {
    List(IgmgPre(pdr = pdr, service = "IGMGPRE", date = Option(date), cau_int_cor = cau_int_cor, readType = None,
      measure = Some(1.0), converted = converted, serialNumberMis = serialNumberMis, serialNumberConv = serialNumberConv, cau_int_mis = None,
      coefCorr = Some(coeffPre), local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None).asInstanceOf[Flow],
      IgmgPost(pdr = pdr, service = "IGMGPOST", date = Option(date), cau_int_cor = cau_int_cor, readType = None,
        measure = Some(1.0), converted = converted, serialNumberMis = serialNumberMis, serialNumberConv = serialNumberConv, cau_int_mis = None,
        coefCorr = Some(coeffPost), local_file = None, d_caricamento = None, isNewRoute = true, pivaDistr = None, pivaUtente = None, ammissibilita = None).asInstanceOf[Flow])
  }

  def generateIgmg(pdr: String, date: String, cau_int_cor: Option[Int]): List[Flow] = {
    generateIgmg(pdr, formatter.parse(date), cau_int_cor)
  }

  def generateIm1(pdr: String, date: String, cau_int_cor: Option[Int]): List[Flow] = {
    generateIm1(pdr, formatter.parse(date), cau_int_cor)
  }

}
