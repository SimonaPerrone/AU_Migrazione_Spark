package it.sferanet.au.controller.ca

import it.sferanet.au.controller.ca.ConsumptionController._
import it.sferanet.au.controller.ca.forzature.ForcingController
import it.sferanet.au.controller.coeffCorr.CoeffCorrController
import it.sferanet.au.controller.coeffCorr.CoeffCorrController.getCoeff
import it.sferanet.au.model.Flow.FlowExtension
import it.sferanet.au.model.MeasureValueType._
import it.sferanet.au.model._
import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.flowTypes.Rettifica
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._
import it.sferanet.au.utilities.Constants.ERROR
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit.DAYS
import java.time.{LocalDate, YearMonth, ZoneId}

class ConsumptionController extends Serializable {
  @transient
  lazy val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  /** Processo di calcolo del consumo annuo */
  def execute(measure: RDD[Flow], rcuTech: RDD[RcuGasMassivoTech], rcusCa: RDD[RcuGasMassivo], rcuProfiloRDD: RDD[RcuGasProfilo]):
  RDD[(String, (IndexedSeq[Consumption], Iterable[RcuGasMassivoTech], Iterable[RcuGasMassivo], Double, Iterable[RcuGasProfilo]))] = {

    /** Controller per la gestione del coefficiente di correzione */
    val coeffController = new CoeffCorrController
    /** Controller per la gestione delle forzature */
    val forcingController = new ForcingController

    val measuresWithCoeff: RDD[Flow] = forcingController
      .putForcingCodeToMeasures(measure) //aggiungo le forzature alle misure
      .map(m => ((m.pdr, m.date, m.service), m))
      .leftOuterJoin(coeffController.get(measure)) // ottengo la mappa misure-coefficienti: v._1 chiave (m.pdr,m.date,m.service)  v._2._1 flusso, v._2._2 coeff
      .map(v => v._2._1.changeCoeff(v._2._2)) // aggiungo i coefficienti causati da flussi CM all'RDD di misure

    /** Anno, equivalente all'anno precedente a [[Environment.getFlowEndDate]], utilizzato per creare i mesi determinati da [[Environment.getCeMeanRange]]. */
    val yearPreviousToFlowEndDate = YearMonth.parse(Environment.getFlowEndDate, DateTimeFormatter.ofPattern("yyyyMM")).minusYears(1).getYear

    /** Lista dei mesi, determinati da [[Environment.getCeMeanRange]] e [[yearPreviousToFlowEndDate]], utilizzati per il calcolo del prelievo giornaliero medio */
    val monthsRange = Environment.getCeMeanRange
      .split(",")
      .map(_.toInt)
      .toList
      .map(YearMonth.of(yearPreviousToFlowEndDate, _))
      .map(month => {
        val startDate = month.atDay(1)
        val endDate = month.atEndOfMonth()
        (startDate, endDate)
      })

    val infoProfiloJoin = rcusCa.keyBy(_.n_id_pdr).groupByKey()
      .join(rcuProfiloRDD.keyBy(_.n_id_pdr).groupByKey())
      .map(v => (v._2._1.head.t_codice_pdr, (v._2._1, v._2._2)))

    measuresWithCoeff.keyBy(_.pdr).groupByKey()
      .leftOuterJoin(rcuTech.keyBy(_.t_codice_pdr).groupByKey())
      .join(infoProfiloJoin)
      // ho generato un paired RDD (<cod_pdr>,((<misura>:Flow,<rcutech>:RcuGasMassivoTech),<rcusCa>:RcuGasMassivo))
      // quindi v._1 chiave => cod_pdr
      // v._2._1._1 è la lista di misure di un pdr, lista di tipo Flow
      // v._2._1._1 è la lista di oggetti di tipo RcuGasMassivoTech associati al pdr (info tecniche coefficiente correzione, num cifre misuratore e convertitore,...)
      // v._2._2 è la lista di oggetti di tipo RcuGasMassivo associati al pdr (info categoria d'uso, regione climatica...)
      //Questa map, appiattisce questa struttura in modo tale da non doverla più andare al leggere in modo così complesso con indicizzazione della tupla annidata
      .map(v => {
        (v._1, v._2._1._1, if (v._2._1._2.isDefined) v._2._1._2.get else List[RcuGasMassivoTech](), v._2._2._1, v._2._2._2)
      })
      .map {
        case (pdr, measure, rcusTech, rcuMassivo, rcusProfilo) =>
          //se rgl o tgl unito con tal tas e tav questi 3 vanno scartati
          val typesFlow = measure.map(f => f.service).toList.distinct
          val validMeasure = if (typesFlow.containsTypes(List(Tgl.serviceName, Rgl.serviceName, Tal.serviceName, Tas.serviceName, Tav.serviceName))
            && (typesFlow.contains(Tgl.serviceName) || typesFlow.contains(Rgl.serviceName))) {
            measure.filter(f => f.service != Tal.serviceName && f.service != Tas.serviceName && f.service != Tav.serviceName)
          } else {
            measure
          }

          // ordino i flussi in ordine temporale decrescente nel caso di im1/igmg il flusso post è più prioritario del pre
          val measureSorted = validMeasure.toList.sorted(Flow.temporalOrderingFlows).reverse

          if (Constants.DEBUG)
            measureSorted.foreach(f => println(f.service + " " +
              f.pdr + " " +
              new java.sql.Date(f.date.get.getTime) + " " +
              f.measure + " " + f.converted))

          var lastConsumption: Consumption = null

          var endMeasure: Flow = if (measureSorted.isEmpty) null else measureSorted.head

          val result = for (z <- 1 until measureSorted.length) yield { // Scorro l'array da più recente al meno recente.
            val startMeasure = measureSorted(z)

            val startRcu: RcuGasMassivoTech = rcusTech.getByDate(startMeasure.date.get)
            val endRcu: RcuGasMassivoTech = rcusTech.getByDate(endMeasure.date.get)
            val lastRcu = rcuMassivo.maxBy(_.startDate)

            // ottengo il coefficiente del consumo nel flusso se definito da un flusso CM
            // in rcuEnd altrimenti (segnante di dx) e se non definito in rcuEnd in rcuStart
            val coeff = getCoeff(flow = endMeasure, rcuStart = startRcu, rcuEnd = endRcu)

            //Ottengo t_misuratore_integrato e t_pre_conv della misura di sinistra
            val (startRcu_t_misuratore_integrato, startRcu_t_pre_conv) = if (startRcu != null)
              (startRcu.t_misuratore_integrato, startRcu.t_pre_conv)
            else
              (None, None)

            //Ottengo t_misuratore_integrato e t_pre_conv della misura di destra
            val (endRcu_t_misuratore_integrato, endRcu_t_pre_conv) = if (endRcu != null)
              (endRcu.t_misuratore_integrato, endRcu.t_pre_conv)
            else
              (None, None)

            //verifico che le matricole tra i due flussi siano uguali o null (AU-612). In altre parole, la condizione fallisce soltanto se sono tutte e quattro non null e diverse a coppie
            val checkSerialNum = (startMeasure.serialNumberMis.isEmpty || endMeasure.serialNumberMis.isEmpty) ||
              (startMeasure.serialNumberConv.isEmpty || endMeasure.serialNumberConv.isEmpty) ||
              (startMeasure.serialNumberMis.orNull == endMeasure.serialNumberMis.orNull) ||
              (startMeasure.serialNumberConv.orNull == endMeasure.serialNumberConv.orNull)

            if (checkSerialNum) {
              val (consumptionPreCoerence, isErrorState): (ConsumptionPreCoerence, ConsumptionErrorStates.Value) =
                try {
                  (startMeasure, endMeasure) match {
                    //Consumo misura pre-intervento e IgmgPre
                    case (startMeasure: Any, endMeasure: IgmgPre) =>
                      (ConsumptionController.getConsumptionPreIgmg(startMeasure, endMeasure), ConsumptionErrorStates.MissingValues)

                    //Consumo misura pre-intervento e IgmrPre
                    case (startMeasure: Any, endMeasure: IgmrPre) =>
                      (ConsumptionController.getConsumptionPreIgmr(startMeasure, endMeasure), ConsumptionErrorStates.MissingValues)

                    //Consumo misura pre-intervento e Im1Pre
                    case (startMeasure: Any, endMeasure: Im1Pre) =>
                      (ConsumptionController.getConsumptionPreIm(startMeasure, endMeasure), ConsumptionErrorStates.MissingValues)

                    //Consumo misura post-intervento e IgmgPost
                    case (startMeasure: IgmgPost, endMeasure: Any) =>
                      (ConsumptionController.getConsumptionPostIgmg(startMeasure, endMeasure, endRcu), ConsumptionErrorStates.MissingValues)

                    //Consumo misura post-intervento e IgmrPost
                    case (startMeasure: IgmrPost, endMeasure: Any) =>
                      (ConsumptionController.getConsumptionPostIgmr(startMeasure, endMeasure, endRcu), ConsumptionErrorStates.MissingValues)

                    //Consumo misura post-intervento e Im1Post
                    case (startMeasure: Im1Post, endMeasure: Any) =>
                      (ConsumptionController.getConsumptionPostIm(startMeasure, endMeasure), ConsumptionErrorStates.MissingValues)

                    //Consumo di due misure non Im1 o Igmg
                    case (startMeasure: Any, endMeasure: Any) =>
                      (ConsumptionController.getConsumptionsDefault(startMeasure, startRcu, endMeasure, endRcu), ConsumptionErrorStates.MissingValues)
                  }
                } catch {
                  case _: NotImplementedException =>
                    (null, ConsumptionErrorStates.NotConfigured)
                  case ex: Throwable =>
                    log.error("Exception creation consumption: %s".format(ex.getMessage))
                    (null, ConsumptionErrorStates.Exception)
                  //throw ex
                }

              if (consumptionPreCoerence != null) {

                //label tipo_coefficiente = CM se il coefficiente del consumo proviene da un Im1/IGMG, tipo_coefficiente = RCU altrimenti
                val coeffLabel = if (startMeasure.coef.isEmpty) CoeffLabel.RCU.toString else CoeffLabel.CM.toString

                //label tipo_forzatura
                val forcingLabel = consumptionPreCoerence.forcingType

                //applico la coerenza dimensionale
                val (start: Double, end: Double, coerenceLabel: String, coerenceFlag: Boolean) =
                  applyDimensionalCoerence(startMeasure, endMeasure, coeff, consumptionPreCoerence.startMeasureType,
                    consumptionPreCoerence.endMeasureType)
                val errorState = if (coerenceLabel == ERROR) ConsumptionErrorStates.MissingValues else ConsumptionErrorStates.None

                //Consumption di OUTPUT
                if (checkSerialNumbers(startMeasure, endMeasure, coerenceFlag)) {
                  lastConsumption = Consumption(
                    pdr,
                    startMeasure.service,
                    endMeasure.service,
                    new Timestamp(startMeasure.date.get.getTime),
                    new Timestamp(endMeasure.date.get.getTime),
                    start,
                    end,
                    startMeasure.local_file,
                    endMeasure.local_file,
                    errorState,
                    Some(coeff),
                    startRcu_t_misuratore_integrato,
                    endRcu_t_misuratore_integrato,
                    startRcu_t_pre_conv,
                    endRcu_t_pre_conv,
                    Some(lastRcu.t_cod_profilo),
                    lastRcu.n_prelievo_annuo,
                    coeffLabel,
                    coerenceLabel,
                    forcingLabel
                  )
                  endMeasure = startMeasure
                }
                else { //se non è valido creo un Consumption con flag isValid false
                  //Consumption di ERRORE ConsumptionErrorStates.SerialNumberMismatch
                  lastConsumption = Consumption(pdr,
                    startMeasure.service, endMeasure.service,
                    new Timestamp(startMeasure.date.get.getTime), new Timestamp(endMeasure.date.get.getTime),
                    0.0, 0.0, startMeasure.local_file, endMeasure.local_file, ConsumptionErrorStates.SerialNumberMismatch, Some(coeff), startRcu_t_misuratore_integrato, endRcu_t_misuratore_integrato, startRcu_t_pre_conv, endRcu_t_pre_conv, Some(lastRcu.t_cod_profilo), lastRcu.n_prelievo_annuo)
                  endMeasure = startMeasure
                }
              } else {
                //Consumption di ERRORE generico
                lastConsumption = Consumption(pdr,
                  startMeasure.service, endMeasure.service,
                  new Timestamp(startMeasure.date.get.getTime), new Timestamp(endMeasure.date.get.getTime),
                  0.0, 0.0, startMeasure.local_file, endMeasure.local_file, isErrorState, Some(coeff), startRcu_t_misuratore_integrato, endRcu_t_misuratore_integrato, startRcu_t_pre_conv, endRcu_t_pre_conv, Some(lastRcu.t_cod_profilo), lastRcu.n_prelievo_annuo)
              }

            } else { //se non è valido creo un Consumption con flag isValid false

              //Consumption di ERRORE ConsumptionErrorStates.SerialNumberMismatch
              lastConsumption = Consumption(pdr,
                startMeasure.service, endMeasure.service,
                new Timestamp(startMeasure.date.get.getTime), new Timestamp(endMeasure.date.get.getTime),
                0.0, 0.0, startMeasure.local_file, endMeasure.local_file, ConsumptionErrorStates.SerialNumberMismatch, Some(coeff), startRcu_t_misuratore_integrato, endRcu_t_misuratore_integrato, startRcu_t_pre_conv, endRcu_t_pre_conv, Some(lastRcu.t_cod_profilo), lastRcu.n_prelievo_annuo)
              endMeasure = startMeasure
            }

            lastConsumption
          }

          if (result.count(v => v == null) > 0) {
            (pdr, null)
          }
          else {
            /** Prelievo medio calcolato per i mesi definiti da [[Environment.getCeMeanRange]] (in genere Maggio, Giugno, Luglio, Settembre) */
            val ceMean = ConsumptionController.ceMean(monthsRange, result)

            // se startservice == Im1/IGMG/IGMR Pre && endservice == Im1/IGMG/IGMR Post non rappresenta un segmento di consumo e quindi va scartato.
            val consumptions = result.filter(v => if (isPre(v.startService) && isPost(v.endService)) false else true) //scarto anche i Consumption non validi

            (pdr, (consumptions, rcusTech, rcuMassivo, ceMean, rcusProfilo))
          }
      }
  }

}

object ConsumptionController {

  case class ConsumptionPreCoerence(startMeasureType: MeasureValueType.Value,
                                    endMeasureType: MeasureValueType.Value,
                                    forcingType: Option[String])

  /** Applica la coerenza dimensionale tra la misura di sinistra e la misura di destra. La coerenza dimensionale è utilizzata per scegliere come calcolare i consumi:
   * ad esempio, utilizzando il prelevato P (altrimenti detto misurato) o il convertito C. La lettera K indica che è da utilizzare la formula 3, che utilizza il valore del pprofk per calcolare il consumo. */
  def applyDimensionalCoerence(startMeasure: Flow, endMeasure: Flow, coeff: Double, startMeasureType: MeasureValueType.Value,
                               endMeasureType: MeasureValueType.Value): (Double, Double, String, Boolean) = {
    // in caso di applicazione di una forzatura in precedenza startMeasureType sarà uguale a endMeasureType per definizione di forzatura in quanto automaticamente coerente dimensionalmente
    val convertedDefined = startMeasure.converted.isDefined && endMeasure.converted.isDefined
    val prelevatedDefined = startMeasure.measure.isDefined && endMeasure.measure.isDefined

    (startMeasureType, endMeasureType, prelevatedDefined, convertedDefined) match {
      case (P, P, true, _) => (startMeasure.measure.get, endMeasure.measure.get, "PPP", true)
      case (P, C, _, true) => (startMeasure.converted.get, endMeasure.converted.get, "PCC", false)
      case (P, C, true, false) => (startMeasure.measure.get, endMeasure.measure.get, "PCP", true)
      case (P, K, true, _) => (startMeasure.measure.get * coeff, endMeasure.measure.get * coeff, "PKK", true)
      case (C, C, _, true) => (startMeasure.converted.get, endMeasure.converted.get, "CCC", false)
      case (C, C, true, false) => (startMeasure.measure.get * coeff, endMeasure.measure.get * coeff, "CCK", true)
      case (C, P, _, true) => (startMeasure.converted.get, endMeasure.converted.get, "CPC", false)
      case (C, P, true, false) => (startMeasure.measure.get, endMeasure.measure.get, "CPP", true)
      case (C, K, _, true) => (startMeasure.converted.get, endMeasure.converted.get, "CKC", false)
      case (C, K, true, false) => (startMeasure.measure.get * coeff, endMeasure.measure.get * coeff, "CKK", true)
      case (K, K, true, _) => (startMeasure.measure.get * coeff, endMeasure.measure.get * coeff, "KKK", true)
      case (K, P, true, _) => (startMeasure.measure.get * coeff, endMeasure.measure.get * coeff, "KPK", true)
      case (K, C, _, true) => (startMeasure.converted.get, endMeasure.converted.get, "KCC", false)
      case (K, C, true, false) => (startMeasure.measure.get * coeff, endMeasure.measure.get * coeff, "KCK", true)

      case everythingElse => (0.0, 0.0, ERROR, true)
    }

  }


  /**
   * confronto le matricole mis e conv tra i flussi start e end
   *
   * @param startMeasure
   * @param endMeasure
   * @return
   */
  def checkSerialNumbers(startMeasure: Flow, endMeasure: Flow, coerenceLabel: Boolean): Boolean = {
    val isSerialNumberMisNull = startMeasure.serialNumberMis.isEmpty || endMeasure.serialNumberMis.isEmpty
    val isSerialNumberConvNull = startMeasure.serialNumberConv.isEmpty || endMeasure.serialNumberConv.isEmpty
    val areSerialNumbersMisEqual = startMeasure.serialNumberMis.orNull == endMeasure.serialNumberMis.orNull
    val areSerialNumbersConvEqual = startMeasure.serialNumberConv.orNull == endMeasure.serialNumberConv.orNull

    (coerenceLabel && (isSerialNumberMisNull || areSerialNumbersMisEqual)) ||
      (!coerenceLabel && (isSerialNumberConvNull || areSerialNumbersConvEqual))
  }

  /**
   * Scelgo la misura prelevata (P), convertita (C) o prelevata per il coefficiente di correzione (K)
   * delle segnanti sinistra e destra delle misure di tipo Flow (non IM1 non IGMG)
   *
   * @param startMeasure [[Flow]] misura di sinistra
   * @param startRcu     [[RcuGasMassivoTech]] dati tecnici della misura di sinistra
   * @param endMeasure   [[Flow]] misura di destra
   * @param endRcu       [[RcuGasMassivoTech]] dati tecnici della misura di destra
   * @return [[ConsumptionPreCoerence]] che contiene un elemento tra {P,C,K} per la scelta della segnante di sinistra
   *         un elemento tra {P,C,K} per la scelta della segnante di destra
   *         String codice forzatura */
  def getConsumptionsDefault(startMeasure: Flow, startRcu: RcuGasMassivoTech, endMeasure: Flow, endRcu: RcuGasMassivoTech): ConsumptionPreCoerence = {

    val forcingCode = if (endMeasure.forcing.isDefined) endMeasure.forcing else startMeasure.forcing
    val forcing = ForcingController.getForcingCode(forcingCode) // C, P o K a seconda del codice forzatura (es IM1PRE3 -> C)

    //se la forzatura e' definita prelevo C,P o K a seconda di quanto dettato dalla forzatura
    if (forcing.isDefined)
      getConsumptionForced(startMeasure, endMeasure, forcing.get, forcingCode.get)
    else
      ConsumptionPreCoerence(
        startMeasureType = getDefault(startMeasure, startRcu), //Consumo di sx in base a startRcu (gruppo_mis_int, pre_conv)
        endMeasureType = getDefault(endMeasure, endRcu), //Consumo di dx in base a startRcu (gruppo_mis_int, pre_conv)
        None)
  }

  /**
   * Scelgo la misura prelevata (P), convertita (C) o prelevata per il coefficiente di correzione (K)
   * di una segnante in base all'elemento di tipo RcuGasMassivoTech (che contiene t_misuratore_integrato,t_pre_conv)
   *
   * @param measure [[Flow]] misura
   * @param rcu     [[RcuGasMassivoTech]] record tecnico di rcu che contiene gruppo_mis_int (t_misuratore_integrato) e pre_conv (t_pre_conv)
   * @return [[MeasureValueType.Value]] P se scelgo la prelevata, C se scelgo la convertita,
   *         K se scelgo la prelevata per il coefficiente di correzione
   */
  def getDefault(measure: Flow, rcu: RcuGasMassivoTech): MeasureValueType.Value = {
    measure match {
      //rettifiche
      case rettifica: Rettifica => getConsumptionRettifica(rettifica, rcu)

      //altri flussi
      case _ => getConsumption(measure, rcu)
    }
  }

  /**
   * Implementa la tabella 6.2	per i Flussi di misura Periodici/Prestazionali
   *
   * @param flow [[Flow]] misura
   * @param rcu  [[RcuGasMassivoTech]] record tecnico di rcu che contiene gruppo_mis_int (t_misuratore_integrato) e pre_conv (t_pre_conv)
   * @return [[MeasureValueType.Value]] P se scelgo la prelevata, C se scelgo la convertita,
   *         K se scelgo la prelevata per il coefficiente di correzione
   */
  def getConsumption(flow: Flow, rcu: RcuGasMassivoTech): MeasureValueType.Value = { // TABELLA 6 del documento
    val GRUPPO_MIS_INT: Option[String] = if (rcu != null) rcu.t_misuratore_integrato else None
    val PRE_CONV: Option[String] = if (rcu != null) rcu.t_pre_conv else None

    GRUPPO_MIS_INT match {
      case Some("SI") => PRE_CONV match {
        case Some("SI") => K
        case Some("NO") => K
        case None => K
        case _ => throw new NotImplementedException
      }
      case Some("NO") =>
        PRE_CONV match {
          case Some("SI") => C
          case Some("NO") => K
          case _ => throw new NotImplementedException
        }
      case None =>
        PRE_CONV match {
          case None => if (flow.converted.isDefined) C else K
          case Some("SI") => C
          case Some("NO") => P
          case _ => throw new NotImplementedException
        }
      case Some("N") =>
        PRE_CONV match {
          case Some("NO") => K
          case Some("SI") => C
          case _ => throw new NotImplementedException
        }
      case Some("S") =>
        PRE_CONV match {
          case Some("NO") => P
          case _ => throw new NotImplementedException
        }
      case Some("") =>
        PRE_CONV match {
          case Some("SI") => C
          case Some("NO") => P
          case _ => throw new NotImplementedException
        }
      case _ => throw new NotImplementedException
    }
  }

  /**
   * Implementa la tabella 9, capitolo 6.5	per i Flussi di rettifica
   *
   * @param flow [[Flow]] misura
   * @param rcu  [[RcuGasMassivoTech]] record tecnico di rcu che contiene gruppo_mis_int (t_misuratore_integrato) e pre_conv (t_pre_conv)
   * @return [[MeasureValueType.Value]] P se scelgo la prelevata, C se scelgo la convertita,
   *         K se scelgo la prelevata per il coefficiente di correzione
   */
  def getConsumptionRettifica(flow: Flow, rcu: RcuGasMassivoTech): MeasureValueType.Value = {
    val GRUPPO_MIS_INT: Option[String] = if (rcu != null) rcu.t_misuratore_integrato else None
    val PRE_CONV: Option[String] = if (rcu != null) rcu.t_pre_conv else None

    GRUPPO_MIS_INT match {
      case Some("SI") => PRE_CONV match {
        case Some("SI") => K
        case Some("NO") => K
        case None => K
        case _ => throw new NotImplementedException
      }
      case Some("NO") =>
        PRE_CONV match {
          case Some("SI") => C
          case Some("NO") => K
          case _ => throw new NotImplementedException
        }
      case None =>
        PRE_CONV match {
          case None => if (flow.converted.isDefined) C else K
          case Some("SI") => C
          case Some("NO") => P
          case _ => throw new NotImplementedException
        }
      case Some("N") =>
        PRE_CONV match {
          case Some("NO") => K
          case Some("SI") => C
          case _ => throw new NotImplementedException
        }
      case Some("S") =>
        PRE_CONV match {
          case Some("NO") => P
          case _ => throw new NotImplementedException
        }
      case Some("") =>
        PRE_CONV match {
          case Some("SI") => C
          case Some("NO") => P
          case _ => throw new NotImplementedException
        }
      case _ => throw new NotImplementedException
    }
  }

  /**
   * Controlla se il flusso in ingresso è una rettifica di tipo Rml o Rgl con motivazione 4 oppure 5
   *
   * @param flow
   * @return True se il flusso in ingresso è un RML o RGL con motivazione 4 o 5, False altrimenti
   */
  def isRettificaRmlRgl4o5(flow: Flow): Boolean = {
    val motivation = flow match {
      case rml: Rml => rml.motivation
      case rgl: Rgl => rgl.motivation
      case _ => None
    }
    motivation == Some(4) || motivation == Some(5)
  }

  /**
   * valuta se il flusso in ingresso è un Im1 o Igmg rettificato (cioè con fileRettifica valorizzato)
   *
   * @param endFlow
   * @return True se Im1/Igmg rettificato, False altrimenti
   */
  def isIm1IgmgRettificato(flow: Flow): Boolean = {
    isCMFlow(flow.service) && flow.fileRettifica.isDefined && flow.fileRettifica.get != ""
  }

  /**
   * valuta se il consumo e' composto da due RGL/RML con motivazione 4/5
   * (sovrascrivono la forzatura scegliendo la convertita (C))
   *
   * @param startFlow [[Flow]] misura di sinistra
   * @param endFlow   [[Flow]] misura di destra
   * @return true se misura di destra e di sinistra sono entrambi un RGL o RML con motivazione 4 o 5
   */
  def forcingOverwritten(startFlow: Flow, endFlow: Flow): Boolean = {
    (isRettificaRmlRgl4o5(startFlow) || isIm1IgmgRettificato(startFlow)) &&
      (isRettificaRmlRgl4o5(endFlow) || isIm1IgmgRettificato(endFlow))
  }

  /**
   * Scelgo la misura prelevata (P), convertita (C) o prelevata per il coefficiente di correzione (K)
   * in base alla forzatura definita per la misura
   * A MENO CHE LA FORZATURA NON SIA SOVRASCRITTA DA RGL/RML mot 4/5 o IM1/IGMG rettificato in tal caso -> C
   *
   * @param startFlow   [[Flow]] misura di sinistra
   * @param endFlow     [[Flow]] misura di destra
   * @param forcing     [[MeasureValueType.Value]] tipo di misura dettato da forzatura in {P,C,K}
   * @param forcingMode [[String]] codice forzatura (es "IGMGPRE2", "IM1PRE5")
   * @return [[ConsumptionPreCoerence]] che contiene un elemento tra {P,C,K} per la scelta della segnante di sinistra
   *         un elemento tra {P,C,K} per la scelta della segnante di destra
   *         String codice forzatura
   * */
  def getConsumptionForced(startFlow: Flow, endFlow: Flow, forcing: MeasureValueType.Value, forcingMode: String): ConsumptionPreCoerence = {
    // Se il consumo e' composto da due RGL o RML con motivazioni 4 o 5 la forzatura e' sovrascritta e scelgo la Convertita (C)
    if (forcingOverwritten(startFlow, endFlow))
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.C, endMeasureType = MeasureValueType.C, forcingType = Some(forcingMode))

    //altrimenti ritorno la misura dettata dalla forzatura (è la stessa sia a destra che a sinistra) tra {P,C,K}
    else
      ConsumptionPreCoerence(startMeasureType = forcing, endMeasureType = forcing, forcingType = Some(forcingMode))
  }

  /**
   * Scelgo la misura prelevata (P), convertita (C) o prelevata per il coefficiente di correzione (K)
   * della misura pre-intervento e del flusso di cambio misuratore IGMG in base a cau_int_cor dell'IGMG
   * (Per tutta la parte pre- intervento non viene effettuato il check con RCUGAS)
   *
   * @param startFlow [[Flow]] misura pre-intervento
   * @param endFlow   [[IgmgPre]] misura IGMGPRE
   * @return [[ConsumptionPreCoerence]] che contiene un elemento tra {P,C,K} per la scelta della segnante di sinistra
   *         un elemento tra {P,C,K} per la scelta della segnante di destra
   *         String codice forzatura
   * */
  def getConsumptionPreIgmg(startFlow: Flow, endFlow: IgmgPre): ConsumptionPreCoerence = {
    val cau_int_cor = endFlow.cau_int_cor

    val forcing = ForcingController.getForcingCode(startFlow.forcing)

    //Se la misura pre-intervento e' forzata prelevo {P,C,K} dettato dalla forzatura
    if (forcing.isDefined) getConsumptionForced(startFlow, endFlow, forcing.get, startFlow.forcing.get)

    else if (converterIgmgInstallingOrAligning(cau_int_cor)) // se cau_int_cor.get == 3 || cau_int_cor.get == 4 -> K
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.K, endMeasureType = MeasureValueType.K, forcingType = None)
    else
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.C, endMeasureType = MeasureValueType.C, forcingType = None)
  }

  def getConsumptionPreIgmr(startFlow: Flow, endFlow: IgmrPre): ConsumptionPreCoerence = {
    val cau_int_cor = endFlow.cau_int_cor

    val forcing = ForcingController.getForcingCode(startFlow.forcing)

    //Se la misura pre-intervento e' forzata prelevo {P,C,K} dettato dalla forzatura
    if (forcing.isDefined) getConsumptionForced(startFlow, endFlow, forcing.get, startFlow.forcing.get)

    else if (converterIgmrInstallingOrAligning(cau_int_cor)) // se cau_int_cor.get == 3 || cau_int_cor.get == 4 -> K
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.K, endMeasureType = MeasureValueType.K, forcingType = None)
    else
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.C, endMeasureType = MeasureValueType.C, forcingType = None)
  }

  /**
   * Implementata la tabella a pag 39 del documento per la Sez post dell'intervallo IGMGPOST - MISURA
   * (con IGMG con cau_int_cor non valorizzato oppure 1)
   * NOTA: Manca da specificare un comportamento di default, nel caso in cui GRUPPO_MIS_INT,PRE_CONV non
   * soddisfino nessuna delle condizioni nella tabella
   *
   * @param rcu [[RcuGasMassivoTech]] su cui controllare i valori di GRUPPO_MIS_INT,PRE_CONV per determinare se
   *            scegliere la segnante prelevata (P), la segnante prelevata * coefficiente di correzione (K)
   *            o la segnante convertita(C)
   * @return [[MeasureValueType.Value]] P se vengono scelte le segnanti prelevate
   *         K se vengono scelte le segnanti prelevate * coefficiente di correzione
   *         C se vengono scelte le segnanti convertite
   */
  def checkRCUGASForIGMGPostMeasure(rcu: RcuGasMassivoTech): MeasureValueType.Value = {

    val GRUPPO_MIS_INT: Option[String] = if (rcu != null) rcu.t_misuratore_integrato else None
    val PRE_CONV: Option[String] = if (rcu != null) rcu.t_pre_conv else None
    (GRUPPO_MIS_INT, PRE_CONV) match {
      case (Some("SI"), _) => K
      case (Some("NO"), Some("SI")) => C
      case (Some("NO"), Some("NO")) => K
      case (None, None) => P
      case (None, Some("SI")) => C
      case (Some("N"), Some("NO")) => K
      case (Some("N"), Some("SI")) => C
      case (Some("S"), Some("NO")) => P
      case (Some(""), Some("SI")) => C
      case (Some(""), Some("NO")) => P
      case (None, Some("NO")) => P

      //MANCA LA CASISTICA DI DEFAULT
      case (_, _) => throw new NotImplementedException
    }
  }

  def checkRCUGASForIGMRPostMeasure(rcu: RcuGasMassivoTech): MeasureValueType.Value = {

    val GRUPPO_MIS_INT: Option[String] = if (rcu != null) rcu.t_misuratore_integrato else None
    val PRE_CONV: Option[String] = if (rcu != null) rcu.t_pre_conv else None
    (GRUPPO_MIS_INT, PRE_CONV) match {
      case (Some("SI"), _) => K
      case (Some("NO"), Some("SI")) => C
      case (Some("NO"), Some("NO")) => K
      case (None, None) => P
      case (None, Some("SI")) => C
      case (Some("N"), Some("NO")) => K
      case (Some("N"), Some("SI")) => C
      case (Some("S"), Some("NO")) => P
      case (Some(""), Some("SI")) => C
      case (Some(""), Some("NO")) => P
      case (None, Some("NO")) => P

      //MANCA LA CASISTICA DI DEFAULT
      case (_, _) => throw new NotImplementedException
    }
  }

  def isIgmgPostRCUGAS(cau_int_cor: Option[Int]): Boolean = cau_int_cor.isEmpty || cau_int_cor == Some(1)

  def isIgmrPostRCUGAS(cau_int_cor: Option[Int]): Boolean = cau_int_cor.isEmpty || cau_int_cor == Some(1)

  /**
   * Scelgo la misura prelevata (P), convertita (C) o prelevata per il coefficiente di correzione (K)
   * del flusso di cambio misuratore IGMGPOST e della misura post-intervento e  in base a cau_int_cor dell'IGMG
   * E se cau_int_cor è 1 o non valorizzato in base al check con RCUGAS (dati tecnici alla data della misura di destra)
   * (Per tutta la parte pre- intervento non viene effettuato il check con RCUGAS)
   *
   * @param startFlow [[IgmgPost]] misura di sinistra
   * @param endFlow   [[Flow]] misura di destra
   * @param rcu       [[RcuGasMassivoTech]] dati tecnici della misura di destra
   *                  su cui controllare i valori di GRUPPO_MIS_INT,PRE_CONV se cau_int_cor = 1 o non valorizzato,
   *                  per scegliere la segnante prelevata (P), la segnante prelevata * coefficiente di correzione (K)
   *                  o la segnante convertita(C)
   * @return [[ConsumptionPreCoerence]] che contiene un elemento tra {P,C,K} per la scelta della segnante di sinistra
   *         un elemento tra {P,C,K} per la scelta della segnante di destra
   *         String codice forzatura
   * */
  def getConsumptionPostIgmg(startFlow: IgmgPost, endFlow: Flow, rcu: RcuGasMassivoTech): ConsumptionPreCoerence = {
    val cau_int_cor = startFlow.cau_int_cor

    val forcing = ForcingController.getForcingCode(endFlow.forcing)

    //Se la misura pre-intervento e' forzata prelevo {P,C,K} dettato dalla forzatura
    if (forcing.isDefined) getConsumptionForced(startFlow, endFlow, forcing.get, endFlow.forcing.get)

    else {
      val measureToTake = {
        //se cau_int_cor = 1 o non valorizzato controllo rcu (gruppo_mis_int e pre_conv) secondo la tabella cap 6.3.2.1
        if (isIgmgPostRCUGAS(cau_int_cor)) checkRCUGASForIGMGPostMeasure(rcu);
        else if (converterIgmgRemoved(cau_int_cor)) K; //se cau_int_cor=2 -> K
        else C
      }
      ConsumptionPreCoerence(measureToTake, measureToTake, None)
    }
  }

  def getConsumptionPostIgmr(startFlow: IgmrPost, endFlow: Flow, rcu: RcuGasMassivoTech): ConsumptionPreCoerence = {
    val cau_int_cor = startFlow.cau_int_cor

    val forcing = ForcingController.getForcingCode(endFlow.forcing)

    //Se la misura pre-intervento e' forzata prelevo {P,C,K} dettato dalla forzatura
    if (forcing.isDefined) getConsumptionForced(startFlow, endFlow, forcing.get, endFlow.forcing.get)

    else {
      val measureToTake = {
        //se cau_int_cor = 1 o non valorizzato controllo rcu (gruppo_mis_int e pre_conv) secondo la tabella cap 6.3.2.1
        if (isIgmrPostRCUGAS(cau_int_cor)) checkRCUGASForIGMRPostMeasure(rcu);
        else if (converterIgmrRemoved(cau_int_cor)) K; //se cau_int_cor=2 -> K
        else C
      }
      ConsumptionPreCoerence(measureToTake, measureToTake, None)
    }
  }


  /**
   * Scelgo la misura prelevata (P), convertita (C) o prelevata per il coefficiente di correzione (K)
   * della misura pre-intervento e del flusso di cambio misuratore IM1 in base a cau_int_cor dell'IM1 (cap 6.3)
   *
   * @param startFlow [[Flow]] misura di sinistra
   * @param endFlow   [[Im1Pre]] misura di desta
   * @return [[ConsumptionPreCoerence]] che contiene un elemento tra {P,C,K} per la scelta della segnante di sinistra
   *         un elemento tra {P,C,K} per la scelta della segnante di destra
   *         String codice forzatura
   * */
  def getConsumptionPreIm(startFlow: Flow, endFlow: Im1Pre): ConsumptionPreCoerence = {
    val cau_int_cor = endFlow.cau_int_cor

    val forcing = ForcingController.getForcingCode(startFlow.forcing)

    //Se la misura pre-intervento e' forzata prelevo {P,C,K} dettato dalla forzatura
    if (forcing.isDefined) getConsumptionForced(startFlow, endFlow, forcing.get, startFlow.forcing.get)

    else if (converterIm1Dissaligned(cau_int_cor)) { //se cau_int_cor = 5 -> K
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.K, endMeasureType = MeasureValueType.K, None)
    } else
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.C, endMeasureType = MeasureValueType.C, None)
  }

  // Ritorna true se l'intervento allinea o installa virtualmente il correttore
  //      -> nella sez pre se true non posso usare segnante convertito -> K
  //      -> nella sez post uso C
  def converterIgmgInstallingOrAligning(cau_int_cor: Option[Int]): Boolean = {
    cau_int_cor.isDefined && (cau_int_cor.get == 3 || cau_int_cor.get == 4)
  }

  def converterIgmrInstallingOrAligning(cau_int_cor: Option[Int]): Boolean = {
    cau_int_cor.isDefined && (cau_int_cor.get == 3 || cau_int_cor.get == 4)
  }

  // Ritorna true se l'intervento rimuove il correttore
  //      -> nella sez pre uso C
  //      -> nella sez post se true NON posso usare il segnante convertito -> K
  def converterIgmgRemoved(cau_int_cor: Option[Int]): Boolean = {
    cau_int_cor.isDefined && (cau_int_cor.get == 2)
  }

  def converterIgmrRemoved(cau_int_cor: Option[Int]): Boolean = {
    cau_int_cor.isDefined && (cau_int_cor.get == 2)
  }

  // Ritorna true se l'intervento rimuove virtualmente o fisicamente il correttore
  //      -> nella sez pre uso C
  //      -> nella sez post non posso usare il segnante convertito -> K
  def converterIm1Removed(cau_int_cor: Option[Int]): Boolean = {
    cau_int_cor.isDefined && (cau_int_cor.get == 3 || cau_int_cor.get == 4)
  }

  //Ritorna true se l'intervento allinea il correttore
  //      -> nella sez pre non posso usare il segnante convertito -> K
  //      -> nella sez post uso C
  def converterIm1Dissaligned(cau_int_cor: Option[Int]): Boolean = {
    cau_int_cor.isDefined && (cau_int_cor.get == 5)
  }

  /**
   * Scelgo la misura prelevata (P), convertita (C) o prelevata per il coefficiente di correzione (K)
   * del flusso di cambio misuratore IM1POST e della misura post-intervento in base a cau_int_cor dell'IM1
   *
   * @param startFlow [[Im1Post]] misura di sinistra
   * @param endFlow   [[Flow]] misura di destra
   * @return [[ConsumptionPreCoerence]] che contiene un elemento tra {P,C,K} per la scelta della segnante di sinistra
   *         un elemento tra {P,C,K} per la scelta della segnante di destra
   *         String codice forzatura
   * */
  def getConsumptionPostIm(startFlow: Im1Post, endFlow: Flow): ConsumptionPreCoerence = {
    val cau_int_cor: Option[Int] = startFlow.cau_int_cor

    val forcing = ForcingController.getForcingCode(endFlow.forcing)
    //Se la misura pre-intervento e' forzata prelevo {P,C,K} dettato dalla forzatura
    if (forcing.isDefined) getConsumptionForced(startFlow, endFlow, forcing.get, endFlow.forcing.get)

    else if (converterIm1Removed(cau_int_cor)) { // se cau_int_cor =3 || cau_int_cor=4 -> K
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.K, endMeasureType = K, None)
    } else
      ConsumptionPreCoerence(startMeasureType = MeasureValueType.C, endMeasureType = C, None)
  }

  /** Calcola il prelievo giornaliero medio per i mesi detarminati da [[Environment.getCeMeanRange]] (per ora sono sempre stati Maggio, Giugno, Luglio, Settembre).
   * Nel caso in cui un segmento sia in parte esterno a tali mesi, viene effettuato un riproporzionamento dei consumi (AU-490, parte 2). */
  def ceMean(monthsRange: List[(LocalDate, LocalDate)], consumptions: IndexedSeq[Consumption]): Double = {
    val filteredConsumptions = consumptions.map({ c =>
      val doesIntersect = monthsRange
        .map({ case (start, end) =>
          val startDateSegment = c.startSegment.toInstant.atZone(ZoneId.systemDefault()).toLocalDate
          val endDateSegment = c.endSegment.toInstant.atZone(ZoneId.systemDefault()).toLocalDate

          val condition = !(
            endDateSegment.isBefore(start) ||
              startDateSegment.isAfter(end)
            )

          val maxStartDate = if (start.isAfter(startDateSegment)) start else startDateSegment
          val minEndDate = if (end.isBefore(endDateSegment)) end else endDateSegment

          val isStartSegmentInMonth = startDateSegment.getMonthValue.equals(minEndDate.getMonthValue)
          val numberOfDays = if (isStartSegmentInMonth)
            Math.max(0, DAYS.between(maxStartDate, minEndDate))
          else Math.max(0, DAYS.between(maxStartDate, minEndDate) + 1)

          (condition, numberOfDays)
        })

      (c, doesIntersect)
    })
      .filter({ case (c, doesIntersect) => doesIntersect.exists(_._1) })
      .map({
        case (c, doesIntersect) =>
          val consumptionPerDay = if (c.getNumberOfDays <= 0) 0.0 else c.getConsumption(null).getOrElse(0.0) / c.getNumberOfDays

          val result = doesIntersect.map({ case (condition, numberOfDays) =>
            (if (condition) consumptionPerDay * numberOfDays else 0.0, numberOfDays)
          })

          val actualConsumption = result.map(_._1).sum
          val actualNumberOfDays = result.map(_._2).sum

          (c, actualConsumption, actualNumberOfDays)
      })

    val totalConsumption = filteredConsumptions.map({ case (_, actualConsumption, _) => actualConsumption }).sum
    val totalNumberOfDays = filteredConsumptions.map({ case (_, _, actualNumberOfDays) => actualNumberOfDays }).sum

    val ceMean = if (totalNumberOfDays > 0) totalConsumption / totalNumberOfDays else 0.0

    ceMean
  }

  @Deprecated
  def ceMeanDeprecated(monthsRange: List[(LocalDate, LocalDate)], consumptions: IndexedSeq[Consumption], rcus: Iterable[RcuGasMassivoTech]): Double = {
    val filteredConsumptions = consumptions.filter(c =>
      monthsRange.exists({ case (start, end) =>
        !(
          c.endSegment.toInstant.atZone(ZoneId.systemDefault()).toLocalDate.isBefore(start) ||
            c.startSegment.toInstant.atZone(ZoneId.systemDefault()).toLocalDate.isAfter(end)
          )
      })
    )

    val sumConsumption = filteredConsumptions.map(c => {
      val consumption = c.getConsumption(rcus.getByDate(c.endSegment))
      consumption.getOrElse(0.0)
    }).sum
    val sumDays = filteredConsumptions.map(c => c.getNumberOfDays).sum
    if (sumDays > 0)
      sumConsumption / sumDays
    else 0.0
  }

  private def isCMFlow(service: String): Boolean = isPre(service) || isPost(service)

  private def isPre(service: String): Boolean = {
    service == Im1Pre.serviceName || service == IgmgPre.serviceName || service == IgmrPre.serviceName
  }

  private def isPost(service: String): Boolean = {
    service == Im1Post.serviceName || service == IgmgPost.serviceName || service == IgmrPost.serviceName
  }

}