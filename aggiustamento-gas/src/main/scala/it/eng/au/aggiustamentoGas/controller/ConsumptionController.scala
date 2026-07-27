package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.model.agg.{Consumption, ErrorEnum, ExternalDailyInfo, FlowWithInfo}
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg._
import it.eng.au.aggiustamentoGas.model.measure.measureTypes.RettificaFlow
import it.eng.au.aggiustamentoGas.model.rcugas._
import it.eng.au.aggiustamentoGas.utility.constants.FieldConstants._
import it.eng.au.aggiustamentoGas.utility.constants.{DimensionalType, Treatment, TreatmentConstant}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/** Controller adibito al calcolo dei consumi */
class ConsumptionController extends Serializable {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  /**
   * Compute segments associating to each measure its next measure for each pdr.
   *
   * @param groupedMeasures an RDD of flowsWithInfo for each pdr
   * @return and RDD[K,V] where key K is the pdr and value V are all the associated segments List[(FlowWithInfo, FlowWithInfo)])
   */
  def calcCouple(groupedMeasures: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))]): RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))] = {
    groupedMeasures
      .mapValues({ case (orderedFlows, externalInfos) =>
        val segments = orderedFlows.zip(if (orderedFlows.nonEmpty) orderedFlows.tail else List[FlowWithInfo]()) //Create couples
        //split im1/igmg into pre and post
        val segmentsFinal = splitIm1Igmg2PrePost(segments, 0)
        (segmentsFinal, externalInfos)
      })
  }

  /**
   * Compute daily consumptions according to specs.
   *
   * @param pdrCouples                   all the segments for a pdr in the computing period. A segment is a measure and its next available
   *                                     measure
   * @param tabProfiliGiorniStdPercBCMap a map where the key is &lt;date, cod_prof_std, id_zona_clim&gt; and the value is pprofk%
   * @return an RDD[Consumption] where each consumption is computed with one of the 3 available formulas (see specs) and
   *         with the dimensional coherence criterion applied
   */
  def calcDailyConsumptions(pdrCouples: RDD[(String, (List[(FlowWithInfo, FlowWithInfo)], ExternalDailyInfo))]
                            , classiGruppiDiMisuraPortataRcugasGet: Broadcast[Map[String, Int]]
                            , startDatePeriod: String = Environment.getPeriodStartDate
                            , endDatePeriod: String = Environment.getPeriodEndDate
                            , sterilizeIsEnable: Boolean = false
                           ): RDD[(String, (List[Consumption], ExternalDailyInfo))] = {
    val computationStartDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime(startDatePeriod).dayOfMonth().withMinimumValue().withTimeAtStartOfDay()
    val computationEndDate = DateTimeFormat.forPattern("yyyyMM").parseDateTime(endDatePeriod).dayOfMonth().withMaximumValue().withTimeAtStartOfDay()

    val xMultiplierMaxRange = Environment.getxMultiplierMaxRange.toDouble
    val yMultiplierConsumptionSterilize = Environment.getyMultiplierConsumptionSterilize.toDouble

    val dailyConsumption =
      pdrCouples.map({ case (pdr, (segmentList, externalDailyInfos)) =>

        if (segmentList.isEmpty) { //no measure for pdr
          (pdr,
            (applyCAFormula(pdr, computationStartDate.minusDays(1), forwardFlag = true, computationEndDate, externalDailyInfos)
              .map(c => c.copy(errorCode = c.errorCode ++ Array(ErrorEnum.PDR_WITHOUT_SEGMENTS_ERROR_CODE))),
              externalDailyInfos)
          )
        } else { //at least one segment exists for pdr
          val lastMeasure = segmentList.last._2
          val firstMeasure = segmentList.head._1
          val startMeasures = segmentList.map(_._1)
          //if the last measure for a pdr is before the computationEndDate we compute consumptions from
          // lastMeasure.date to computationEndDate using the 3rd formula (profiling with CA)
          // NB. computationEndDate.plusDays(1) is required otherwise when lastMeasure.date == computationEndDate the
          //     consumption for this date is not created
          val lastConsumptionsProfiled: List[Consumption] =
          if (lastMeasure.flow.date.get.withTimeAtStartOfDay().isBefore(computationEndDate)) {
            applyCAFormula(pdr, lastMeasure.flow.date.get, forwardFlag = true, computationEndDate.plusDays(1), externalDailyInfos)
          } else {
            List()
          }
          //if the first measure for a pdr is after the computationStartDate we compute consumptions from
          // computationStartDate to firstMeasure.date using the 3rd formula (profiling with CA)
          // NB. computationStartDate.minusDays(1) is required otherwise when firstMeasure.date > computationEndDate the
          //     consumption for computationEndDate is not created
          val firstConsumptionsProfiled: List[Consumption] =
          if (firstMeasure.flow.date.get.withTimeAtStartOfDay().isAfter(computationStartDate) || firstMeasure.flow.date.get.withTimeAtStartOfDay().isEqual(computationStartDate)) {
            val consumi = applyCAFormula(pdr, firstMeasure.flow.date.get, forwardFlag = false, computationStartDate.minusDays(1), externalDailyInfos)

            /**
             * CR - Gabrini Federico - 16/12/2021 - add checkActivation
             */
            checkActivations(startMeasures, consumi, externalDailyInfos)
          } else {
            List()
          }
          //All the other segments either use G formula or Y/M formula.
          val segmentsConsumptions = segmentList.flatMap({ case (startMeasure, endMeasure) =>
            computeConsumptions(startMeasure, endMeasure, externalDailyInfos)
          })

          val activationConsumption = checkActivations(startMeasures, segmentsConsumptions, externalDailyInfos)

          (pdr, (firstConsumptionsProfiled ++ lastConsumptionsProfiled ++ activationConsumption, externalDailyInfos))
        }
      })
        .mapValues({ case (consumptionsList, externalDailyInfos) =>
          (consumptionsList.filter(c => DateUtility.isBetween(c.date, computationStartDate, computationEndDate)), externalDailyInfos)
        }).mapValues({ case (consumptionsList, externalDailyInfos) =>
        (consumptionsList.map(sanitizeNegativeConsumptions), externalDailyInfos)
      })
    if (sterilizeIsEnable)
      dailyConsumption
        .mapValues({ case (consumptionsList, externalDailyInfos) =>
          (sterilizeIncoerenti(consumptionsList, externalDailyInfos, xMultiplierMaxRange, yMultiplierConsumptionSterilize, classiGruppiDiMisuraPortataRcugasGet.value), externalDailyInfos)
        })
    else
      dailyConsumption
  }

  def specificConditionFormula2ShouldNotFormula4(flow1: FlowWithInfo, flow2: FlowWithInfo): Boolean = {
    flow1.monthTreatment.exists(_.treatment.equals(Treatment.G.toString)) || flow2.monthTreatment.exists(_.treatment.equals(Treatment.G.toString))
  }

  /** <p>Replaces Im1 or Igmg as end measure with a Pre instance. Replaces Im1 or Igmg as start measure with a Post instance.
   * If there are the conditions to apply the special formula for im1/igmg the split is not performed.</p>
   *
   * @param orderedFlows             the segments
   * @param recursiveCallAccumulator <p>a counter of recursive call made. When valued with 0 means we are examining the
   *                                 first segment of the segment-list associated to a pdr. The zero case requires a
   *                                 special management when the left measure of the first segment is istnance of Im1Igmg.</p>
   * @return <p> a new segment where (_, PRE) if the input was (FLOW, IM1/IGMG), (POST,_ ) if the input was (IM1/IGMG, FLOW),
   *         (FLOW, IM1/IGMG),  (IM1/IGMG.SameDayFLOW, FLOW)  if the input was  (FLOW, IM1/IGMG), (IM1/IGMG, FLOW) and
   *         they are at 1 day distance between each other,
   *         (startMeasure, endMeasure) otherwise.</p>
   */
  def splitIm1Igmg2PrePost(orderedFlows: List[(FlowWithInfo, FlowWithInfo)], recursiveCallAccumulator: Int): List[(FlowWithInfo, FlowWithInfo)] = {
    if (orderedFlows.size < 1) { //base case
      Nil
    } else if (orderedFlows.size == 1) { //base case
      val (firstSegmentStartFWI, firstSegmentEndFWI) = orderedFlows.head

      if (recursiveCallAccumulator == 0) {
        // we need to split im1/igmg if it is the first measure (first means left-measure of the very first segment)
        // otherwise the next step will ignore it
        (firstSegmentStartFWI.flow, firstSegmentEndFWI.flow) match {
          case (s1: Im1Igmg, e1: Im1Igmg) =>
            List((firstSegmentStartFWI.copy(flow = s1.post), firstSegmentEndFWI.copy(flow = e1.pre)))
          case (s1: Im1Igmg, e1: Flow) =>
            List((firstSegmentStartFWI.copy(flow = s1.post), firstSegmentEndFWI))
          case (s1: Flow, e1: Im1Igmg) =>
            List((firstSegmentStartFWI, firstSegmentEndFWI.copy(flow = e1.pre)))
          case (_, _) => orderedFlows
        }
      } else
        (firstSegmentStartFWI.flow, firstSegmentEndFWI.flow) match {

          case (s1: Flow, e1: Im1Igmg) if (DateUtility.daysBetween(s1.date.get, e1.date.get) == 1) && e1.sameDayFlow.isDefined && specificConditionFormula2ShouldNotFormula4(firstSegmentStartFWI, firstSegmentEndFWI) =>
            //Formula 4 splitting
            if (e1.post.isCorrected && isMot4OrMot5(e1.post.correctionFlow.get)) {
              List((firstSegmentStartFWI, firstSegmentEndFWI.copy(flow = e1.setSameDayFlow(e1.post))))
            } else {
              List((firstSegmentStartFWI, firstSegmentEndFWI))
            }
          case (s1: Flow, e1: Im1Igmg) => //STD split
            List((firstSegmentStartFWI, firstSegmentEndFWI.copy(flow = e1.pre)))
          case (s1: Im1Igmg, e1: Flow) => //STD split
            List((firstSegmentStartFWI.copy(flow = s1.post), firstSegmentEndFWI))
          case (_, _) => orderedFlows
        }
    } else if (recursiveCallAccumulator == 0) { //first inductive step:
      // we need to split im1/igmg if it is the first measure (first means left-measure of the very first segment)
      // otherwise the next step will ignore it
      val (firstSegmentStartFWI, firstSegmentEndFWI) = orderedFlows.head
      (firstSegmentStartFWI.flow, firstSegmentEndFWI.flow) match {
        case (s1: Im1Igmg, e1: Flow) =>
          splitIm1Igmg2PrePost(List((firstSegmentStartFWI.copy(flow = s1.post), firstSegmentEndFWI)) ++ orderedFlows.tail, recursiveCallAccumulator + 1)
        case (_, _) => splitIm1Igmg2PrePost(orderedFlows, recursiveCallAccumulator + 1)
      }
    }
    else { //general inductive step
      val (firstSegmentStartFWI, firstSegmentEndFWI) = orderedFlows.head
      val (secondSegmentStartFWI, secondSegmentEndFWI) = orderedFlows.tail.head
      (firstSegmentStartFWI.flow, firstSegmentEndFWI.flow, secondSegmentStartFWI.flow, secondSegmentEndFWI.flow) match {
        case (s1: Flow, e1: Im1Igmg, s2: Im1Igmg, e2: Flow)
          if (DateUtility.daysBetween(s1.date.get, e1.date.get) == 1) && specificConditionFormula2ShouldNotFormula4(firstSegmentStartFWI, firstSegmentEndFWI) &&
            (DateUtility.daysBetween(s2.date.get, e2.date.get) == 1) && specificConditionFormula2ShouldNotFormula4(secondSegmentStartFWI, secondSegmentEndFWI) &&
            e1.sameDayFlow.isDefined =>
          if (e1.post.isCorrected && isMot4OrMot5(e1.post.correctionFlow.get)) { // if Post is corrected by a mot4,5 then ARU case, else go as usual
            //assigning  Post instance to Im1Igmg.SameDayFlow to apply correctly the scenarios for Rml/Rgl with mot 4 or 5
            List((firstSegmentStartFWI, firstSegmentEndFWI.copy(flow = e1.setSameDayFlow(e1.post)))) ++
              splitIm1Igmg2PrePost(List((secondSegmentStartFWI.copy(flow = s2.post), secondSegmentEndFWI)) ++ orderedFlows.tail.tail, recursiveCallAccumulator + 1) //recursive call
          } else {
            List((firstSegmentStartFWI, firstSegmentEndFWI)) ++
              splitIm1Igmg2PrePost(List((secondSegmentStartFWI.copy(flow = s2.sameDayFlow.get), secondSegmentEndFWI)) ++ orderedFlows.tail.tail, recursiveCallAccumulator + 1) //recursive call
          }
        case (s1: Flow, e1: Im1Igmg, s2: Im1Igmg, e2: Flow) =>
          List((firstSegmentStartFWI, firstSegmentEndFWI.copy(flow = e1.pre))) ++
            splitIm1Igmg2PrePost(List((secondSegmentStartFWI.copy(flow = s2.post), secondSegmentEndFWI)) ++ orderedFlows.tail.tail, recursiveCallAccumulator + 1)
        case (s1: Flow, e1: Flow, s2: Flow, e2: Flow) =>
          List(orderedFlows.head) ++ splitIm1Igmg2PrePost(orderedFlows.tail, recursiveCallAccumulator + 1)
      }
    }
  }

  @deprecated("La sterilizzazione dei consumi è affidata al processo di pubblicazione (vedere progetto aggregatore-consumi-agg).")
  def sterilizeIncoerenti(consumptions: List[Consumption], externalDailyInfo: ExternalDailyInfo, xMultiplierMaxRange: Double, yMultiplierConsumptionSterilize: Double, classiGruppiDiMisuraPortataRcugasGet: Map[String, Int]): List[Consumption] = {
    consumptions
      .map(c => {

        val rcuTech = externalDailyInfo.findRcuGasTech(c.date).flatMap(_.classeMisuratore)
        val getRangeMax = classiGruppiDiMisuraPortataRcugasGet.get(rcuTech.getOrElse(""))

        if (getRangeMax.nonEmpty && (c.value.getOrElse(0.0) > (xMultiplierMaxRange * getRangeMax.get))) {
          c.copy(valueNotSterilized = c.value, value = Some(yMultiplierConsumptionSterilize * getRangeMax.get))
        }
        else c
      })
  }

  /**
   * Sanitizes a Consumption instance by ensuring that negative consumption values are handled appropriately.
   *
   * If the consumption value is defined and less than 0, the method modifies the consumption instance by setting
   * the value to 0.0 and adding an error code indicating that the consumption is negative.
   *
   * @param c the Consumption instance to sanitize
   * @return a sanitized Consumption instance
   */
  def sanitizeNegativeConsumptions(c: Consumption): Consumption = {
    if (c.value.isDefined && (c.value.get < 0))
      c.copy(
        value = Some(0.0),
        errorCode = c.errorCode ++ Array(ErrorEnum.CONSUMPTION_IS_NEGATIVE_ERROR_CODE)
      )
    else c
  }

  private def isMot4OrMot5(f: RettificaFlow): Boolean = {
    f match {
      case rgl: Rgl => Set(4, 5).contains(rgl.motivation.getOrElse(-1))
      case rml: Rml => Set(4, 5).contains(rml.motivation.getOrElse(-1))
      case _ => false
    }
  }

  /**
   * <p>Tells whether or not the serial number of the segment are coherent. A couple where start measure serial and end
   * measure serial are both None is labeled as having equals serial numbers. If mis_serial is coherent and conv_serial
   * is not then the function return false i.e. it considers the couple as not coherent.</p>
   * <p><b>IT RETURNS ALWAYS TRUE IF THE SEGMENT IS (POST,_) OR (_,PRE)</b></p>
   *
   * @param startMeasure the left side of a segment
   * @param endMeasure   the right side of a segment
   * @return a flag telling is the serial numbers match. Not valued serial numbers are considered matching serials
   *
   *
   *         CR - Gabrini Federico - 16/12/2021 - add verify mismatch matricole to IGMG
   *         CR - Gabrini Federico - 09/06/2022 - The check matricole serial num IGMG is done after
   */
  def areSerialNumbersCoherent(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Boolean = {
    val serialNumEqual: (Option[String], Option[String]) => Boolean = {
      case (None, None) => true
      case (None, y: Some[String]) => true
      case (x: Some[String], None) => true
      case (x: Some[String], y: Some[String]) => x == y
    }

    val equalityCondition =
      (startMeasure.dimensionalType, endMeasure.dimensionalType) match {

        case (Some(DimensionalType.P), Some(DimensionalType.P)) |
             (Some(DimensionalType.PK), Some(DimensionalType.P)) |
             (Some(DimensionalType.P), Some(DimensionalType.PK)) =>
          endMeasure.flow match {
            //The check matricole serial num IGMG is done after
            //            case igmg: Igmg => serialNumEqual(startMeasure.flow.serialNumberMis, igmg.pre.serialNumberMis) &&
            //              igmg.sameDayFlow.isDefined &&
            //              serialNumEqual(igmg.sameDayFlow.get.serialNumberMis, igmg.post.serialNumberMis)
            case _ => serialNumEqual(startMeasure.flow.serialNumberMis, endMeasure.flow.serialNumberMis)
          }
        //TODO da rivedere formula 4
        case (Some(DimensionalType.C), Some(DimensionalType.C)) /*| (Some(DimensionalType.H), Some(DimensionalType.H))*/ =>
          endMeasure.flow match {
            //The check matricole serial num IGMG is done after
            //            case igmg: Igmg => c &&
            //              igmg.sameDayFlow.isDefined &&
            //              serialNumEqual(igmg.sameDayFlow.get.serialNumberConv, igmg.post.serialNumberConv)
            case _ => serialNumEqual(startMeasure.flow.serialNumberConv, endMeasure.flow.serialNumberConv)
          }

        case (_, _) => serialNumEqual(startMeasure.flow.serialNumberMis, endMeasure.flow.serialNumberMis)
      }

    //    ((startMeasure.flow.isInstanceOf[Im1Pre] || startMeasure.flow.isInstanceOf[Im1Post] || startMeasure.flow.isInstanceOf[Im1]) && !(endMeasure.flow.isInstanceOf[IgmgPre] || endMeasure.flow.isInstanceOf[IgmgPost] || endMeasure.flow.isInstanceOf[Igmg])) ||
    //      (!(startMeasure.flow.isInstanceOf[IgmgPre] || startMeasure.flow.isInstanceOf[IgmgPost] || startMeasure.flow.isInstanceOf[Igmg]) && (endMeasure.flow.isInstanceOf[Im1Pre] || endMeasure.flow.isInstanceOf[Im1Post] || endMeasure.flow.isInstanceOf[Im1])) ||
    //      equalityCondition
    // The check matricole serial num IGMG is done after (view formula 4 condition) but remain the check matricole igmgpre and igmgpost beacuse this aren't use in formula 4
    startMeasure.flow.isInstanceOf[Im1Post] || endMeasure.flow.isInstanceOf[Im1Pre] || endMeasure.flow.isInstanceOf[Im1Igmg] || equalityCondition
  }


  /**
   * <p>Apply treatment G formula using the right field to compute the consumption according to dimensionalType field.
   * <b>Dimensional coherence is a precondition to this function<b></p>
   *
   * @param startMeasure the left side of a segment
   * @param endMeasure   the right side of a segment
   * @return a valid Consumption if serial numbers matches, a null consumption with error code otherwise.
   */
  def computeDifferenceOnlyConsumptionWithSerialCheck(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Consumption = {
    if (areSerialNumbersCoherent(startMeasure, endMeasure)) {
      Consumption.createDifferenceOnlyConsumption(startMeasure, endMeasure, G_FORMULA_ID)
    }
    else {
      val c = Consumption.createNullConsumption(startMeasure, endMeasure, G_FORMULA_ID)
      c.copy(errorCode = c.errorCode ++ Array(ErrorEnum.NON_MATCHING_SERIALS_ERROR_CODE))
    }
  }

  /**
   * <p>Apply treatment Y and M formula using the right field to compute the consumption according to the case:<br><br>
   * C<sub>k+1</sub> = p<sup>%</sup><sub>PROF, k</sub> * (mis<sub>z+1, pdr</sub> - mis<sub>z, pdr</sub>) /
   * (p<sup>%</sup><sub>PROF, i</sub> + p<sup>%</sup><sub>PROF, i+1</sub>, ..., p<sup>%</sup><sub>PROF, i+j</sub>), <br>
   * where [i,j] = all days between z and z+1.</p>
   *
   * <p>For example if z = 1/09/2021 anz z+1 = 5/09/2021 then the interval [i,j] =
   * { 1/09/2021,  2/09/2021,  3/09/2021,  4/09/2021,  5/09/2021}.</p>
   *
   * <p><b>Dimensional coherence is a precondition to this function<b></p>
   *
   * @param startMeasure      The left side of a segment
   * @param endMeasure        The right side of a segment
   * @param pprofMap          A map giving coefficients  p<sup>%</sup><sub>PROF, k</sub> for each day, cod_prof_std, id_zona_clim
   * @param externalDailyInfo rows from rcugas_massivo and rcugas_conn2distr accessible by date
   * @return A list of valid consumption, one per day between startMeasure and endMeasure, if all data are available. If
   *         there are missing data for one day the consumption is set to zero for that day with a proper error code.
   *
   */
  def applyMYFormula(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, externalDailyInfo: ExternalDailyInfo, gcFlag: Boolean, misMatch: Boolean): List[Consumption] = {
    val consumption = computeDifferenceOnlyConsumptionWithSerialCheck(startMeasure, endMeasure)

    if (!((consumption.errorCode.toSet.size == 1) && consumption.errorCode.toSet.contains(ErrorEnum.NO_ERROR_CODE))) { //either serial number doesn't match or a measure is not valued
      createNullConsumptionBetween(startMeasure.flow.date.get, endMeasure.flow.date.get, startMeasure, endMeasure)
        .map(_.copy(errorCode = consumption.errorCode, idFormula = M_Y_FORMULA_ID)) //FILL INTERVAL WITH NULL CONSUMPTION AND PROPER ERROR
        .map(addExternalInfoToConsumption(_, externalDailyInfo))
    }
    else {
      val daysNr = DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get)
      val lastDate = endMeasure.flow.date.get
      /*
      * !!! IMPORTANT !!!
      *
      * THE FOLLOWING STATEMENT IS WRONG ON PURPOSE: ACQUIRENTE UNICO HAS EXPLICITLY REQUEST TO MODIFY THE FORMULA USING
      * A WRONG FORMULATION DESPITE SEVERAL WARNINGS FROM ENGINEERING CONSULTANT TEAM.
      *
      * SEE EMAIL: R: AGG - Evolutive/Modifiche Procedura FROM Salvatore Ferrara IN DATE 07/06/2021, 18:26 AND REPLY TO IT
      * IN DATE 11/06/2021, 17:57
      *
      * THE CORRECT DATES LIST IS:  datesList: List[DateTime] = (0 until daysNr).toList.map(offset => lastDate.minusDays(offset))
      *
      * THE EFFECT OF THIS BEHAVIOUR IS TO USE A WRONG NORMALIZATION COEFFICIENT TO REPARTITION MONTHLY CONSUMPTIONS
      * ACROSS DAYS OF MONTH, RESULTING INTO AN UNDERESTIMATION OF CONSUMPTIONS AND HENCE A CONSUMPTION LOSS ON FINAL
      * METRICS FOR A PDR.
      * THE RIGHT FORMULA IMPLIES THAT THE NORMALIZATION COEFFICIENT IS BUILT EXCLUDING THE DATE OF THE LEFT SIDE OF A
      * CONSUMPTION (startMeasure IN THE CODE) SINCE ITS DATE IS NOT RELEVANT TO REPARTITIONING, ITS VALUE IS THE ONLY
      * THING WE CARE ABOUT.
      *
      * UPDATE 09/01/2024:
      * ACQUIRENTE UNICO HAS REQUESTED A CHANGE IN THE CALCULATION OF FORMULA 2, FOLLOWING A REPORT FROM THE AUTHORITY.
      * THE datesList HAS BEEN UPDATED, EXCLUDING THE DATE ON THE LEFT SIDE OF THE CONSUMPTION.
      * NEW SET OF VALUES: datesList: List[DateTime] = (0 until daysNr).toList.map(offset => lastDate.minusDays(offset))
      * */
      val datesList: List[DateTime] = (0 until daysNr).toList.map(offset => lastDate.minusDays(offset))

      val (pProfList, infos) = getpProfListAndInfosWithSuspended(datesList, externalDailyInfo)

      val consumptions = pProfList.zip(infos).map({
        case (pprofk, (date, conn2Distr, varProfilo, suspended, rcuMassivo, varPrelAnnuo)) =>
          val codProfStd = varProfilo.flatMap(_.tCodProfilo)
          val idRegClim = conn2Distr.flatMap(_.idRegioneClimatica)
          val ca = varPrelAnnuo.flatMap(_.nPrelivevoAnnuo)
          val error = (codProfStd, idRegClim, pprofk, rcuMassivo) match {
            case (_, _, _, None) => ErrorEnum.FURNITURE_INACTIVE_ERROR_CODE
            case (None, _, _, _) => ErrorEnum.COD_PROF_STD_ERROR_CODE
            case (_, None, _, _) => ErrorEnum.ID_REG_CLIM_ERROR_CODE
            //case (_, _, None, _) => ErrorEnum.PPROF_K_ERROR_CODE
            case (_, _, _, _) => ErrorEnum.NO_ERROR_CODE
          }
          consumption.copy(
            value = if (gcFlag) Some(applyGiroContatoreFormula(startMeasure, endMeasure, misMatch)) else if (consumption.value.isDefined) Some(consumption.value.get) else None,
            ca = ca,
            date = date,
            pprof = None,
            idFormula = if (gcFlag) GC_FORMULA_ID else M_Y_FORMULA_ID,
            codProfStd = codProfStd,
            idRegClim = idRegClim,
            errorCode = consumption.errorCode ++ Array(error),
            isPdrSuspended = if (suspended.isDefined) true else false,
            valueF3 = if (ca.isDefined) Some(ca.get) else None,
            dateStartF2 = Some(datesList.last.toString(FLOW_DATE_FORMAT)),
            dateEndF2 = Some(datesList.head.toString(FLOW_DATE_FORMAT))
          )
      })
      consumptions.filter(_.date.isAfter(startMeasure.flow.date.get))
    }
  }

  /**
   * <p>When the right extreme of the segment is missing (endMeasure is not valued) or the left extreme of the segment is
   * missing (startMeasure is not valued), or if a pdr has no measure or if it suspended or it has not treatment we
   * apply the formula:</p>
   * <p>C<sub>k</sub> =  p<sup>%</sup><sub>PROF, k</sub> * CA<sub>pdr</sub></p>
   *
   * <p>When forwardFlag is true we create consumptions from startDate to endComputationDate, otherwise we
   * create consumptions from startComputationDate to startDate.</p>
   *
   * @param pdr               A pdr
   * @param startDate         Date from or to produce
   * @param pprofMap          A map giving coefficients  p<sup>%</sup><sub>PROF, k</sub> for each day, cod_prof_std, id_zona_clim
   * @param forwardFlag       A flag that, if true, tells we want to compute consumptions in the interval (measure,
   *                          computationDate) while if false in the interval (computationDate, measure).
   * @param computationDate   a date needed to compute consumptions (see forwardFlag description to understand usage)
   * @param externalDailyInfo rows from rcugas_massivo and rcugas_conn2distr accessible by date
   * @return A list of valid consumption, one per day between computationDate and measure.date, if all data are available.
   *         if for some day there aren't enough information the consumption is set to null only for that day.
   * */
  def applyCAFormula(pdr: String, startDate: DateTime, forwardFlag: Boolean, computationDate: DateTime, externalDailyInfo: ExternalDailyInfo): List[Consumption] = {
    val daysNr = DateUtility.daysBetween(startDate, computationDate)
    val datesList = (0 until daysNr).toList.map(daysOffset =>
      if (forwardFlag) computationDate.minusDays(daysOffset)
      else startDate.minusDays(daysOffset)
    )
    val (infosChecked, infos) = getpProfListAndInfos(datesList, externalDailyInfo)
    val n_id_pdr = externalDailyInfo.rcuGasMassivoPList.get.find(_.tCodicePdr.equals(pdr)).map(_.nIdPdr)
    val consumptions = infosChecked.zip(infos).map({
      case (pprofk, (date, conn2Distr, varProfilo, suspended, rcuMassivo, varPrelAnnuo)) =>
        val codProfStd = varProfilo.flatMap(_.tCodProfilo)
        val idRegClim = conn2Distr.flatMap(_.idRegioneClimatica)
        val ca = varPrelAnnuo.flatMap(_.nPrelivevoAnnuo)
        val error = (codProfStd, idRegClim, pprofk, ca, rcuMassivo) match {
          case (_, _, _, _, None) => ErrorEnum.FURNITURE_INACTIVE_ERROR_CODE //max priority since it is required from aggregation procedure
          case (None, _, _, _, _) => ErrorEnum.COD_PROF_STD_ERROR_CODE
          case (_, None, _, _, _) => ErrorEnum.ID_REG_CLIM_ERROR_CODE
          //case (_, _, None, _, _) => ErrorEnum.PPROF_K_ERROR_CODE
          case (_, _, _, None, _) => ErrorEnum.CA_NOT_PRESENT_ERROR_CODE
          case (_, _, _, _, _) => ErrorEnum.NO_ERROR_CODE
        }
        val c = Consumption.createNullConsumption(date, pdr)
        c.copy(
          value = if (ca.isDefined) Some(ca.get) else None,
          date = date,
          pprof = None,
          coefficient = if (n_id_pdr.isDefined) externalDailyInfo.findRcuGasTech(date).filter(_.nIdPdr.equals(n_id_pdr.get)).flatMap(_.nCoeffCorr) else c.coefficient,
          codProfStd = codProfStd,
          idRegClim = idRegClim,
          errorCode = c.errorCode ++ Array(error),
          ca = ca,
          pdr = pdr,
          idFormula = CA_FORMULA_ID,
          isPdrSuspended = if (suspended.isDefined) true else false,
          valueF3 = if (ca.isDefined) Some(ca.get) else None
        )
    })
    consumptions
  }

  def getpProfListAndInfos(datesList: List[DateTime]
                           , externalDailyInfo: ExternalDailyInfo
                           , needAllExternalDailyInfo: Boolean = true //there is this for performance only beacuse not all method that call this method they have need of all the externalDailyInfo
                          ): (List[(String, String, Int)], List[(DateTime, Option[RcuGasConnessioniDistr2], Option[RcuGasVarProfiloP], Option[RcuGasSuspendedPdr], Option[RcuGasMassivoP], Option[RcuGasVarPrelAnnuoP])]) = {
    val zippedInfos = datesList.map(date => (
      date,
      externalDailyInfo.findRcuGasConnessioniDistr2(date),
      externalDailyInfo.findRcuGasVarProfilo(date),
      if (needAllExternalDailyInfo) externalDailyInfo.findRcuGasSospensioni(date) else None,
      if (needAllExternalDailyInfo) externalDailyInfo.findRcuGasMassivoP(date) else None,
      externalDailyInfo.findRcuGasVarPrelAnnuo(date)
    ))

    val keys = zippedInfos.map({
      case (date, conn2Distr, varProfilo, suspended, rcuMassivo, varPrelAnnuo) => (
        date.toString("yyyyMMdd"),
        varProfilo.flatMap(_.tCodProfilo).getOrElse(""),
        conn2Distr.flatMap(_.idRegioneClimatica).getOrElse(-100)
      )
    })

    (keys, zippedInfos)
  }

  def getpProfListAndInfosWithSuspended(datesList: List[DateTime]
                           , externalDailyInfo: ExternalDailyInfo
                           , needAllExternalDailyInfo: Boolean = true
                                       ): (List[(String, String, Int)], List[(DateTime, Option[RcuGasConnessioniDistr2], Option[RcuGasVarProfiloP], Option[RcuGasSuspendedPdr], Option[RcuGasMassivoP], Option[RcuGasVarPrelAnnuoP])]) = {
    val zippedInfos = datesList.map(date => (
      date,
      externalDailyInfo.findRcuGasConnessioniDistr2(date),
      externalDailyInfo.findRcuGasVarProfiloWithSuspended(date),
      if (needAllExternalDailyInfo) externalDailyInfo.findRcuGasSospensioni(date) else None,
      if (needAllExternalDailyInfo) externalDailyInfo.findRcuGasMassivoP(date) else None,
      externalDailyInfo.findRcuGasVarPrelAnnuo(date)
    ))

    val keys = zippedInfos.map({
      case (date, conn2Distr, varProfilo, suspended, rcuMassivo, varPrelAnnuo) => (
        date.toString("yyyyMMdd"),
        varProfilo.flatMap(_.tCodProfilo).getOrElse(""),
        conn2Distr.flatMap(_.idRegioneClimatica).getOrElse(-100)
      )
    })

    (keys, zippedInfos)
  }


  /**
   * <p>Compute consumptions till the end of the month for startMeasure. EndMeasure is used only to apply M/Y formula</p>
   * <p>In other words: right-padding the consumptions from the date of endMeasure till the end of month of endMeasure</p>
   *
   * @param startMeasure      Left side of a segment
   * @param endMeasure        Right side of a segment
   * @param pprofMap          A map giving coefficients  p<sup>%</sup><sub>PROF, k</sub> for each day, cod_prof_std, id_zona_clim
   * @param externalDailyInfo Rows from rcugas_massivo and rcugas_conn2distr accessible by date
   * @return consumptions computed with M/Y formula but only between startMeasure date and the last day of month of startMeasure.date
   */
  def padTillEndOfMonth(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, externalDailyInfo: ExternalDailyInfo): List[Consumption] = {
    val endOfMonth = startMeasure.flow.date.get.dayOfMonth().withMaximumValue().withTimeAtStartOfDay()
    applyMYFormula(startMeasure, endMeasure, externalDailyInfo, gcFlag = false, misMatch = false)
      .filter(consumption => {
        (consumption.date.withTimeAtStartOfDay.isBefore(endOfMonth) ||
          consumption.date.withTimeAtStartOfDay().isEqual(endOfMonth)) &&
          (consumption.date.withTimeAtStartOfDay.isAfter(startMeasure.flow.date.get.withTimeAtStartOfDay()) ||
            consumption.date.withTimeAtStartOfDay.isEqual(startMeasure.flow.date.get.withTimeAtStartOfDay()))
      })
  }

  /**
   * <p>Compute consumptions from the first day of the month until endMeasure. StartMeasure is used only to apply M/Y formula</p>
   * <p>In other words: left-padding the consumptions from the start of month of startMeasure till the date of startMeasure</p>
   *
   * @param startMeasure      Left side of a segment
   * @param endMeasure        Right side of a segment
   * @param pprofMap          A map giving coefficients  p<sup>%</sup><sub>PROF, k</sub> for each day, cod_prof_std, id_zona_clim
   * @param externalDailyInfo Rows from rcugas_massivo and rcugas_conn2distr accessible by date
   * @return consumptions computed with M/Y formula but only between the first day of month of endmeasure.date and endMeasure date
   */
  def padFromStartOfMonth(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, externalDailyInfo: ExternalDailyInfo): List[Consumption] = {
    val startOfMonth = endMeasure.flow.date.get.dayOfMonth().withMinimumValue().withTimeAtStartOfDay()
    applyMYFormula(startMeasure, endMeasure, externalDailyInfo, gcFlag = false, misMatch = false)
      .filter(consumption => {
        (consumption.date.withTimeAtStartOfDay.isBefore(endMeasure.flow.date.get.withTimeAtStartOfDay()) ||
          consumption.date.withTimeAtStartOfDay().isEqual(endMeasure.flow.date.get.withTimeAtStartOfDay())) &&
          (consumption.date.withTimeAtStartOfDay.isAfter(startOfMonth.withTimeAtStartOfDay()) ||
            consumption.date.withTimeAtStartOfDay.isEqual(startOfMonth.withTimeAtStartOfDay()))
      })
  }

  /**
   * <p>Creates null valued consumption in the interval defined by startDate (<strong>excluded</strong>) and endDate
   * (<strong>inclusive</strong>), that might be different from the interval defined by statMeasure.date and endMeasure.date .</p>
   * <p><strong>Important:</strong> startMeasure.flow.date.get is excluded in dates range,  endMeasure.flow.date.get is
   * included: dates interval, using calculus notation for intervals, is (startMeasure.flow.date.get, endMeasure.flow.date]</p>
   *
   * @param startDate    the left date to define consumption generation interval
   * @param endDate      the right date to define consumption generation interval
   * @param startMeasure the left side of a segment
   * @param endMeasure   the right side of a segment
   * @return A list of null consumptions
   */
  def createNullConsumptionBetween(startDate: DateTime, endDate: DateTime, startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): List[Consumption] = {
    val consumption = Consumption.createNullConsumption(startMeasure, endMeasure, NULL_CONSUMPTION_FORMULA_ID)
    val daysNr = DateUtility.daysBetween(startDate, endDate)
    val consumptinsList = (0 until daysNr).toList.map((_, consumption)).map({
      case (daysOffset, consumption) =>
        consumption.copy(date = endDate.minusDays(daysOffset), errorCode = consumption.errorCode)
    })
    consumptinsList
  }

  /**
   * <p>Creates zero valued consumption for the segment. Used when treatment is G, segment's extremes belong to the same month
   * but measure dates are not consecutive days</p>
   * <p>If used in a different scenario the error code must be overwritten.</p>
   * <p><strong>Important:</strong> startMeasure.flow.date.get is excluded in dates range,  endMeasure.flow.date.get is
   * included: dates interval, using calculus notation for intervals, is (startMeasure.flow.date.get, endMeasure.flow.date]</p>
   *
   * @param startMeasure the left side of a segment
   * @param endMeasure   the right side of a segment
   * @return A list of zero valued consumptions
   */
  def createZeroValuedConsumptionsInTheInterval(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): List[Consumption] = {
    val consumption = Consumption.createZeroValuedConsumption(startMeasure, endMeasure, NULL_CONSUMPTION_FORMULA_ID)
    val daysNr = DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get)
    val consumptinsList = (0 until daysNr).toList.map((_, consumption))
      .map({
        case (daysOffset, consumption) =>
          consumption.copy(date = endMeasure.flow.date.get.minusDays(daysOffset), errorCode = consumption.errorCode ++ Array(ErrorEnum.NOT_CONSECUTIVE_DAYS_ERROR_CODE))
      })
    consumptinsList
  }

  /**
   * @param c                 a consumption
   * @param externalDailyInfo a collection of historicized information
   * @return c with ca, codProfStd and idRegClim valued if not null at c.date
   */
  def addExternalInfoToConsumption(c: Consumption, externalDailyInfo: ExternalDailyInfo): Consumption = {
    val ca = externalDailyInfo.findRcuGasVarPrelAnnuo(c.date).flatMap(_.nPrelivevoAnnuo)
    val codProfStd = externalDailyInfo.findRcuGasVarProfilo(c.date).flatMap(_.tCodProfilo)
    val idRegClim = externalDailyInfo.findRcuGasConnessioniDistr2(c.date).flatMap(_.idRegioneClimatica)
    val rcuMassivo = externalDailyInfo.findRcuGasMassivoP(c.date)
    val suspended = externalDailyInfo.findRcuGasSospensioni(c.date)

    val error = (ca, codProfStd, idRegClim, rcuMassivo) match { //do not override errors if present
      case (_, _, _, None) => Array(ErrorEnum.FURNITURE_INACTIVE_ERROR_CODE)
      case (_, _, None, _) => Array(ErrorEnum.ID_REG_CLIM_ERROR_CODE)
      case (_, None, _, _) => Array(ErrorEnum.COD_PROF_STD_ERROR_CODE)
      case (None, _, _, _) => Array(ErrorEnum.CA_NOT_PRESENT_ERROR_CODE)
      case _ => Array(ErrorEnum.NO_ERROR_CODE)
    }
    c.copy(
      ca = ca,
      pprof = None,
      codProfStd = codProfStd,
      idRegClim = idRegClim,
      errorCode = c.errorCode ++ error,
      isPdrSuspended = if (suspended.isDefined) true else false,
      valueF3 = if (ca.isDefined) Some(ca.get) else None
    )
  }

  /** <p>Telling if both measures have treatment G.</p>
   *
   * @param startMeasure left side of a segment
   * @param endMeasure   right side of a segment
   * @return True if FlowWithInfo.monthTreatment.treatment is Treatment.G for both measures.
   */
  def bothAreG(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Boolean = {
    startMeasure.monthTreatment.isDefined &&
      startMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) &&
      endMeasure.monthTreatment.isDefined &&
      endMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString)
  }

  /** <p>Telling if both measures are in the same month</p>
   *
   * @param startMeasure left side of a segment
   * @param endMeasure   right side of a segment
   * @return True if startMeasure.year == endMeasure.year and startMeasure.month == endMeasure.month.
   */
  def sameMonth(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Boolean = {
    startMeasure.flow.date.isDefined && endMeasure.flow.date.isDefined &&
      startMeasure.flow.date.get.year.equals(endMeasure.flow.date.get.year) &&
      startMeasure.flow.date.get.monthOfYear.equals(endMeasure.flow.date.get.monthOfYear)
  }

  /**
   * Boolean condition telling if we should apply M Y formula
   *
   * @param startMeasure mis<sub>k, pdr</sub
   * @param endMeasure   mis<sub>k+1, pdr</sub>
   * @return true if we should r apply M Y formula
   */
  def MYFormulaCondition(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Boolean = {
    startMeasure.monthTreatment.isDefined && endMeasure.monthTreatment.isDefined &&
      (startMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) ||
        startMeasure.monthTreatment.get.treatment.equals(Treatment.M.toString) ||
        startMeasure.monthTreatment.get.treatment.equals(Treatment.Y.toString)) &&
      (endMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) ||
        endMeasure.monthTreatment.get.treatment.equals(Treatment.M.toString) ||
        endMeasure.monthTreatment.get.treatment.equals(Treatment.Y.toString))
  }

  /**
   * <p>Apply the correct formula and return a list of consumptions, one for each day in the segment.
   * If conditions to apply the formulas are not met, like specs constraints or missing data, null consumptions with
   * error code are returned in the interval</p>
   * <p><strong>Precondition to this method is that Monthtreatment must be valued for all FlowWithInfo</strong></p>
   *
   * @param startMeasure      mis<sub>k, pdr</sub
   * @param endMeasure        mis<sub>k+1, pdr</sub>
   * @param pprofMap          A map giving coefficients  p<sup>%</sup><sub>PROF, k</sub> for each day, cod_prof_std, id_zona_clim
   * @param externalDailyInfo rows from rcugas_massivo and rcugas_conn2distr accessible by date
   * @return A list of consumptions.
   */
  def computeConsumptions(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, externalDailyInfo: ExternalDailyInfo): List[Consumption] = {
    (startMeasure.flow, endMeasure.flow) match {
      //Scenarios not specified in specs but to be covered
      //Segment without treatment
      case (_: Flow, _: Flow) if startMeasure.monthTreatment.isEmpty || endMeasure.monthTreatment.isEmpty =>
        applyCAFormula(startMeasure.flow.pdr, startMeasure.flow.date.get, forwardFlag = true, endMeasure.flow.date.get, externalDailyInfo)
          .map(c => c.copy(errorCode = c.errorCode ++ Array(ErrorEnum.TREATMENT_IS_NULL_ERROR_CODE)))

      /*case (_: Flow, _: Flow)
        if endMeasure.dimensionalType.contains(DimensionalType.C)
          && (startMeasure.flow.serialNumberConv.getOrElse("start") == endMeasure.flow.serialNumberConv.getOrElse("end")
          && (endMeasure.flow.converted.getOrElse(0.0) - startMeasure.flow.converted.getOrElse(0.0) < 0))
          && (endMeasure.rcuGasVarConvertitore.isDefined && endMeasure.rcuGasVarConvertitore.get.nCifreConv.isDefined)
        //  && (startMeasure.flow.readType.getOrElse(' ') != 'S' && endMeasure.flow.readType.getOrElse(' ') != 'S') //collaudato ma temporaneamente disattivato
      =>
        if (DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get) == 1) {
          List(computeDifferenceOnlyConsumptionWithSerialCheck(startMeasure, endMeasure))
            .map(addExternalInfoToConsumption(_, externalDailyInfo))
            .map(c => c.copy(value = Some(applyGiroContatoreFormula(startMeasure, endMeasure, misMatch = false))))
            .map(c => c.copy(idFormula = GC_FORMULA_ID))
        }
        else {
          applyMYFormula(startMeasure, endMeasure, externalDailyInfo, gcFlag = true, misMatch = false)
        }

      case (_: Flow, _: Flow)
        if (endMeasure.dimensionalType.contains(DimensionalType.PK) || endMeasure.dimensionalType.contains(DimensionalType.P))
          && (startMeasure.flow.serialNumberMis.getOrElse("start") == endMeasure.flow.serialNumberMis.getOrElse("end")
          && endMeasure.flow.measure.getOrElse(0.0) - startMeasure.flow.measure.getOrElse(0.0) < 0)
          && (endMeasure.rcuGasTech.isDefined && endMeasure.rcuGasTech.get.nCifreMis.isDefined)
      //    && (startMeasure.flow.readType.getOrElse(' ') != 'S' && endMeasure.flow.readType.getOrElse(' ') != 'S') //collaudato ma temporaneamente disattivato
      =>
        if (DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get) == 1) {
          List(computeDifferenceOnlyConsumptionWithSerialCheck(startMeasure, endMeasure))
            .map(addExternalInfoToConsumption(_, externalDailyInfo))
            .map(c => c.copy(value = Some(applyGiroContatoreFormula(startMeasure, endMeasure, misMatch = true))))
            .map(c => c.copy(idFormula = GC_FORMULA_ID))
        }
        else {
          applyMYFormula(startMeasure, endMeasure, externalDailyInfo, gcFlag = true, misMatch = true)
        }*/

      //Standard cases from specs (the match-case statement is a literal translation of specification, without optimizations)
      case (_: Tgl, _: Tgl) | (_: Tgl, _: Rgl) | (_: Rgl, _: Tgl) | (_: Rgl, _: Rgl)
        if bothAreG(startMeasure, endMeasure) && sameMonth(startMeasure, endMeasure)
          && DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get) == 1
      => List(computeDifferenceOnlyConsumptionWithSerialCheck(startMeasure, endMeasure)).map(addExternalInfoToConsumption(_, externalDailyInfo))


      case (_: Tgl, _: Tgl) | (_: Tgl, _: Rgl) | (_: Rgl, _: Tgl) | (_: Rgl, _: Rgl)
        if bothAreG(startMeasure, endMeasure) &&
          //right segment is not the first day of month
          (!endMeasure.flow.date.get.withDayOfMonth(1).withTimeAtStartOfDay().equals(endMeasure.flow.date.get.withTimeAtStartOfDay())) &&
          DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get) > 1
      => createZeroValuedConsumptionsInTheInterval(startMeasure, endMeasure).map(addExternalInfoToConsumption(_, externalDailyInfo))

      case (_: Flow, _: Flow) //dx measure is G (at least) and measures (sx,dx) are consecutive (includes im1/igmg scenarios)
        if endMeasure.monthTreatment.isDefined &&
          endMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) &&
          DateUtility.daysBetween(startMeasure.flow.date.get, endMeasure.flow.date.get) == 1
      => List(computeDifferenceOnlyConsumptionWithSerialCheck(startMeasure, endMeasure)).map(addExternalInfoToConsumption(_, externalDailyInfo))

      /* N special cases */
      /*case (_: Flow, _: Flow) //dx and sx are G and between them there is at least a month with treatment N (i.e. a month with no measures)
        if endMeasure.monthTreatment.isDefined && startMeasure.monthTreatment.isDefined &&
          endMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) &&
          startMeasure.monthTreatment.get.treatment.equals(Treatment.G.toString) &&
          DateUtility.monthsDifference(startMeasure.flow.date.get, endMeasure.flow.date.get) > 1
      =>
        val nullConsumptionStartDate = startMeasure.flow.date.get.plusMonths(1).dayOfMonth().withMinimumValue().withTimeAtStartOfDay().minusDays(1)
        val nullConsumptionEndDate = endMeasure.flow.date.get.minusMonths(1).dayOfMonth().withMaximumValue().withTimeAtStartOfDay()
        padTillEndOfMonth(startMeasure, endMeasure, pprofMap, externalDailyInfo) ++
          createNullConsumptionBetween(nullConsumptionStartDate, nullConsumptionEndDate, startMeasure, endMeasure).map(addExternalInfoToConsumption(_, externalDailyInfo, pprofMap)) ++
          padFromStartOfMonth(startMeasure, endMeasure, pprofMap, externalDailyInfo)*/

      case (s: Flow, e: Flow) //sx and dx are N
        if endMeasure.monthTreatment.isDefined && startMeasure.monthTreatment.isDefined &&
          startMeasure.monthTreatment.get.treatment.equals(Treatment.N.toString) &&
          endMeasure.monthTreatment.get.treatment.equals(Treatment.N.toString)
      => createNullConsumptionBetween(startMeasure.flow.date.get, endMeasure.flow.date.get, startMeasure, endMeasure).map(addExternalInfoToConsumption(_, externalDailyInfo))

      case (s: Flow, e: Flow) //sx is N and dx is not
        if endMeasure.monthTreatment.isDefined && startMeasure.monthTreatment.isDefined &&
          startMeasure.monthTreatment.get.treatment.equals(Treatment.N.toString) &&
          (!endMeasure.monthTreatment.get.treatment.equals(Treatment.N.toString))
      =>
        val nullConsumptionStartDate = startMeasure.flow.date.get
        val nullConsumptionEndDate = endMeasure.flow.date.get.minusMonths(1).dayOfMonth().withMaximumValue().withTimeAtStartOfDay()
        createNullConsumptionBetween(nullConsumptionStartDate, nullConsumptionEndDate, startMeasure, endMeasure).map(addExternalInfoToConsumption(_, externalDailyInfo)) ++
          padFromStartOfMonth(startMeasure, endMeasure, externalDailyInfo)

      case (s: Flow, e: Flow) //dx is N and sx is not
        if endMeasure.monthTreatment.isDefined && startMeasure.monthTreatment.isDefined &&
          endMeasure.monthTreatment.get.treatment.equals(Treatment.N.toString) &&
          (!startMeasure.monthTreatment.get.treatment.equals(Treatment.N.toString))
      =>
        val nullConsumptionStartDate = startMeasure.flow.date.get.plusMonths(1).dayOfMonth().withMinimumValue().withTimeAtStartOfDay().minusDays(1)
        val nullConsumptionEndDate = endMeasure.flow.date.get
        padTillEndOfMonth(startMeasure, endMeasure, externalDailyInfo) ++
          createNullConsumptionBetween(nullConsumptionStartDate, nullConsumptionEndDate, startMeasure, endMeasure).map(addExternalInfoToConsumption(_, externalDailyInfo))

      // specific case that can be used differently in agg/sbg
      case (_: Flow, _: Flow)
        if specificSbgConditionFormula2(startMeasure, endMeasure) => specificSbgOutputFormula2(startMeasure, endMeasure, externalDailyInfo)

      // either sx or dx are G and none of the previous condition is met or they are M or Y.
      case (_: Flow, _: Flow) if MYFormulaCondition(startMeasure, endMeasure)
      => applyMYFormula(startMeasure, endMeasure, externalDailyInfo, gcFlag = false, misMatch = false)

      case (_, _) =>
        createNullConsumptionBetween(startMeasure.flow.date.get, endMeasure.flow.date.get, startMeasure, endMeasure)
          .map(c => c.copy(errorCode = c.errorCode ++ Array(ErrorEnum.NOT_IMPLEMENTED_SCENARIO_ERROR_CODE)))
    }
  }

  def applyGiroContatoreFormula(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, misMatch: Boolean): Double = {
    val label = (startMeasure.dimensionalType, endMeasure.dimensionalType) match {
      case (Some(DimensionalType.C), Some(DimensionalType.C)) =>
        if (startMeasure.flow.converted.isDefined && endMeasure.flow.converted.isDefined)
          DimensionalType.C
        else
          DimensionalType.PK

      case (Some(DimensionalType.C), Some(DimensionalType.PK)) =>
        if (startMeasure.flow.converted.isDefined && endMeasure.flow.converted.isDefined)
          DimensionalType.C
        else
          DimensionalType.PK

      case (Some(DimensionalType.PK), Some(DimensionalType.PK)) =>
        DimensionalType.PK

      case (Some(DimensionalType.PK), Some(DimensionalType.C)) =>
        if (startMeasure.flow.converted.isDefined && endMeasure.flow.converted.isDefined)
          DimensionalType.C
        else
          DimensionalType.PK

      case _ => DimensionalType.PK
    }

    if (
      (misMatch && endMeasure.rcuGasTech.forall(_.nCifreMis.isEmpty)) ||
        (!misMatch && endMeasure.rcuGasVarConvertitore.forall(_.nCifreConv.isEmpty))
    ) {
      0
    }
    else {
      val exp = if (!misMatch && label.equals(DimensionalType.C)) endMeasure.rcuGasVarConvertitore.get.nCifreConv.getOrElse(0) else endMeasure.rcuGasTech.get.nCifreMis.getOrElse(0)
      val start = if (!misMatch && label.equals(DimensionalType.C)) startMeasure.flow.converted.getOrElse(0.0) else startMeasure.flow.measure.getOrElse(0.0)
      val end = if (!misMatch && label.equals(DimensionalType.C)) endMeasure.flow.converted.getOrElse(0.0) else endMeasure.flow.measure.getOrElse(0.0)

      val coeff =
        if (label == DimensionalType.PK && startMeasure.im1IgmgCoeff.isDefined && startMeasure.im1IgmgCoeff.getOrElse(1.0) <= 30.0 && startMeasure.im1IgmgCoeff.getOrElse(1.0) >= 0)
          startMeasure.im1IgmgCoeff.getOrElse(1.0)

        else if (label == DimensionalType.PK && startMeasure.coeff.isDefined && startMeasure.coeff.getOrElse(1.0) <= 30.0 && startMeasure.coeff.getOrElse(1.0) >= 0)
          startMeasure.coeff.getOrElse(1.0)

        else
          1.0

      (end + (math.pow(10, exp) - start)) * coeff
    }

  }

  def specificSbgConditionFormula2(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo): Boolean = false

  def specificSbgOutputFormula2(startMeasure: FlowWithInfo, endMeasure: FlowWithInfo, externalDailyInfo: ExternalDailyInfo): List[Consumption] = List()

  /**
   *
   * CR - Gabrini Federico - 16/12/2021 - change calculation mode from first day where there is one activation measure (A01/A40)
   *
   * @param firstMeasure
   * @param consumi
   * @return
   */
  @deprecated
  def checkActivation(firstMeasure: FlowWithInfo, consumi: List[Consumption], externalDailyInfos: ExternalDailyInfo, pprofMap: Map[(String, String, Int), Double]): List[Consumption] = {
    val listActivationFlow = List(classOf[A01], classOf[A01r], classOf[A40], classOf[A40r])

    if ((listActivationFlow.exists(_.isInstance(firstMeasure.flow)) || firstMeasure.flow.activationFlow.isDefined) && firstMeasure.monthTreatment.isDefined) {
      firstMeasure.monthTreatment.get.treatment match {
        case TreatmentConstant.M | TreatmentConstant.Y =>
          consumi.map(_.copy(value = Some(0), idFormula = ACTIVATION_FORMULA_ID, startMeasure = firstMeasure.copy(flow = firstMeasure.flow.activationFlow.getOrElse(firstMeasure.flow)), endMeasure = firstMeasure))
        case TreatmentConstant.G =>
          val listCons = consumi
            .map(_.copy(value = Some(0), idFormula = ACTIVATION_FORMULA_ID, startMeasure = firstMeasure.copy(flow = firstMeasure.flow.activationFlow.getOrElse(firstMeasure.flow)), endMeasure = firstMeasure))
            .sortBy(_.date)(DateUtility.dateTimeOrdering)
          val replaceLast = Consumption.createDifferenceOnlyConsumption(firstMeasure.copy(flow = firstMeasure.flow.activationFlow.getOrElse(firstMeasure.flow)), firstMeasure, ACTIVATION_FORMULA_ID)
          val addExternalInfo = addExternalInfoToConsumption(replaceLast, externalDailyInfos)
          listCons.dropRight(1) :+ addExternalInfo
        case _ => consumi
      }
    }
    else
      consumi
  }

  def checkActivations(
                        measures: List[FlowWithInfo],
                        consumi: List[Consumption],
                        externalDailyInfos: ExternalDailyInfo
                      ): List[Consumption] = {

    val listActivationFlow = List(classOf[A01], classOf[A01r], classOf[A40], classOf[A40r])
    val defaultDate: Option[DateTime] = Some(new DateTime(3000, 1, 1, 0, 0))

    val result = measures.foldLeft(consumi) { (currentConsumi, measure) =>
      if (
        (listActivationFlow.exists(_.isInstance(measure.flow)) || measure.flow.activationFlow.isDefined) &&
          measure.monthTreatment.isDefined
      ) {
        val targetDate = measure.flow.date.get
        val startFlow = measure.flow.activationFlow.getOrElse(measure.flow)

        measure.monthTreatment.get.treatment match {
          case TreatmentConstant.M | TreatmentConstant.Y =>
            currentConsumi.map {
              case c if c.date.isEqual(targetDate) =>
                sanitizeNegativeConsumptions(c).copy(
                  value = Some(0),
                  idFormula = ACTIVATION_FORMULA_ID,
                  startMeasure = measure.copy(flow = startFlow),
                  endMeasure = measure
                )
              case c if {
                val dataDisattivazione =
                  Option(c.endMeasure)
                    .flatMap(em => Option(em.flow))
                    .map {
                      case d: D01 => d.date
                      case d: D01r => d.date
                      case d: D02 => d.date
                      case _ => defaultDate
                    }
                    .getOrElse(defaultDate)
                c.date.isBefore(targetDate) && c.date.isAfter(dataDisattivazione.get)
              } =>
                sanitizeNegativeConsumptions(c).copy(
                  value = Some(0),
                  idFormula = ACTIVATION_FORMULA_ID,
                  startMeasure = measure.copy(flow = startFlow),
                  endMeasure = measure
                )
              case c if {
                val fornituraInattiva = {
                  Option(c.errorCode.contains(ErrorEnum.FURNITURE_INACTIVE_ERROR_CODE))
                }.getOrElse(false)
                c.date.isBefore(targetDate) && fornituraInattiva
              } =>
                sanitizeNegativeConsumptions(c).copy(
                  value = Some(0),
                  idFormula = ACTIVATION_FORMULA_ID,
                  startMeasure = measure.copy(flow = startFlow),
                  endMeasure = measure
                )
              case c => c
            }
          case TreatmentConstant.G =>
            currentConsumi.map {
              case c if c.date == targetDate
                && !Option(c.endMeasure).flatMap(em => Option(em.flow)).exists(flow => listActivationFlow.exists(_.isInstance(flow)))
              =>
                val replaced = sanitizeNegativeConsumptions(Consumption.createDifferenceOnlyConsumption(
                  measure.copy(flow = startFlow),
                  measure,
                  ACTIVATION_FORMULA_ID
                ))
                addExternalInfoToConsumption(replaced, externalDailyInfos)
              case c if {
                val dataDisattivazione =
                  Option(c.endMeasure)
                    .flatMap(em => Option(em.flow))
                    .map {
                      case d: D01 => d.date
                      case d: D01r => d.date
                      case d: D02 => d.date
                      case _ => defaultDate
                    }
                    .getOrElse(defaultDate)
                c.date.isBefore(targetDate) && c.date.isAfter(dataDisattivazione.get)
              } =>
                sanitizeNegativeConsumptions(c).copy(
                  value = Some(0),
                  idFormula = ACTIVATION_FORMULA_ID,
                  startMeasure = measure.copy(flow = startFlow),
                  endMeasure = measure.copy(flow = startFlow)
                )
              case c if {
                val fornituraInattiva = {
                  Option(c.errorCode.contains(ErrorEnum.FURNITURE_INACTIVE_ERROR_CODE))
                }.getOrElse(false)
                c.date.isBefore(targetDate) && fornituraInattiva
              } =>
                sanitizeNegativeConsumptions(c).copy(
                  value = Some(0),
                  idFormula = ACTIVATION_FORMULA_ID,
                  startMeasure = measure.copy(flow = startFlow),
                  endMeasure = measure
                )
              case c => c
            }

          case _ => currentConsumi
        }
      } else {
        currentConsumi
      }
    }
    result
  }
}