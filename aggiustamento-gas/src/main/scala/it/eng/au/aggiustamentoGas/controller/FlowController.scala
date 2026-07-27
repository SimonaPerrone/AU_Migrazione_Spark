package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.dao.measure._
import it.eng.au.aggiustamentoGas.filter.exclusion.ExclusionFilterController
import it.eng.au.aggiustamentoGas.filter.inclusion.InclusionFilterController
import it.eng.au.aggiustamentoGas.model.agg.PdrWithMonthTreatmentYSBG
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Im1, Im1Igmg}
import it.eng.au.aggiustamentoGas.model.measure._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.annotation.tailrec

/**
 * Controller per l'esecuzione dei processi iniziali di lettura e gestione delle misure
 * @param softExclusionFilterController controller per il filtro di esclusione standard
 * @param strongExclusionFilterController controller per il filtro di esclusioen forzata
 * @param inclusionFilters controller per i filtri di inclusione
 */
class FlowController(private val softExclusionFilterController: ExclusionFilterController,
                     private val strongExclusionFilterController: ExclusionFilterController,
                     private val inclusionFilters: List[InclusionFilterController]) {
  /** DAO per le misure di tipo RML (definito a parte perché viene selezionata un'unica RML per PdR e data da [[FlowController.getLatestRml]] */
  val rmlDaoList: List[MeasureDAO] = List(
    new RmlDAO
  )

 /** Lista dei DAO per le misure adibite all'assegnazione del trattamento (RML, RGL, TGL, TML) */
  val treatmentFlowDAOList: List[MeasureDAO] = List(
    new RglDAO,
    new TglDAO,
    new TmlDAO
  )

  /** Lista dei DAO per le misure con motivazione 6 (TAL, TAV, TAS, RGL, RML) */
  val mot6DaoList: List[MeasureDAO] = List(
    new TalDAO,
    new TavDAO,
    new TasDAO,
    new RglDAO
  )

  /** Lista dei DAO per le misure di intervento/cambio misuratore (IM1, IGMG, IGMR) */
  val igmgList: List[MeasureDAO] = List(
    new Im1DAO,
    new IgmgDAO,
    new IgmrDAO
  )

  /** Lista dei DAO per tutte le altre misure */
  val listDAO: List[MeasureDAO] = List(
    new RmvDAO,
    new TmvDAO,
    new RslDAO,
    new Swg1DAO,
    new FuiDAO,
    //new FddDAO, TODO: eventually read them
    new A01rDAO,
    new A01DAO,
    new A40rDAO,
    new A40DAO,
    new D01rDAO,
    new D01DAO,
    new D02rDAO,
    new D02DAO,
    new Sm1rDAO,
    new Sm1DAO,
    new Sm2rDAO,
    new Sm2DAO,
    new Ad2rDAO,
    new Ad2DAO,
    new Ad3rDAO,
    new Ad3DAO,
    new Ad4rDAO,
    new Ad4DAO,
    new Ad5rDAO,
    new Ad5DAO,
    new A02rDAO,
    new A02DAO,
    new S02rDAO,
    new S02DAO,
    new S40rDAO,
    new S40DAO,
    new R01rDAO,
    new R01DAO,
    new R40rDAO,
    new R40DAO,
    new M01rDAO,
    new M01DAO,
    new V01rDAO,
    new V01DAO,
    new V02rDAO,
    new V02DAO,
    new A01DAO,
    new IgmrDAO
  )

  /**
   * Legge le misure contenute in [[daoList]] nell'intervallo [ [[startDate]], [[endDate]] ], applicando alcuni filtri di correttezza e di unicità delle misure (per l'unicità vedere [[FlowController.removeDuplicateFlows]])
   * @param startDate estremo destro di lettura delle misure
   * @param endDate estremo sinistro di lettura delle misure
   * @param daoList misure da leggere
   * @param getTreatment booleano che indica se estrarre il trattamento dalle misure
   * @param rcuTreatment solo in SBG, filtra le misure per trattamento
   * @return RDD[ [[Flow]] ]
   */
  def getMeasures(startDate: String, endDate: String, daoList: List[MeasureDAO], getTreatment: Boolean, rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    //    val defaultPartitioner: Partitioner = new PdrPartitioner(Environment.getNumberPartition.toInt)
    val inputDateFormatter = DateTimeFormat.forPattern("yyyyMM")
    val startDateJoda = inputDateFormatter.parseDateTime(startDate)
    val endDateJoda = inputDateFormatter.parseDateTime(endDate).dayOfMonth().withMaximumValue()
    val ghigliottinaJoda = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(Environment.getFlowGhigliottina)

    val flowRDD = daoList.map(_.get(startDate, endDate, getTreatment))    // estrazione delle misure
      .reduce(_.union(_))                                                 // union delle misure
      .coalesce(Environment.getNumberPartition.toInt)
      .repartition(Environment.getNumberPartition.toInt)
      /* filtri di correttezza delle misure:
      * - data della misura definita
      * - PdR non nullo/vuoto
      * - ammissibilità diversa da BLOCCANTE
      * - motivazione in (3,6), oppure
      *     misura o convertito non nulli, oppure
      *     misura di tipo Im1/Igmg con almeno un valore misura/convertito del pre/post non nullo
      * - data della misura compresa tra startDateJoda e endDateJoda
      * - data di caricamento del file precedente alla data di ghigliottina
       */
      .filter(f => f.date.isDefined && (f.pdr != null && !f.pdr.trim.equals("") && !f.pdr.trim.equals("null")))
      .filter(f => f.ammissibilita.getOrElse("") != "BLOCCANTE")
      .filter(f =>
        Set(3, 6).contains(f.motivation.getOrElse(-1)) ||
          !(f.measure.isEmpty && f.converted.isEmpty) ||
          (f.isInstanceOf[Im1Igmg] && (
            f.asInstanceOf[Im1Igmg].pre.measure.isDefined || f.asInstanceOf[Im1Igmg].pre.converted.isDefined ||
              f.asInstanceOf[Im1Igmg].post.measure.isDefined || f.asInstanceOf[Im1Igmg].post.converted.isDefined
            ))
      )
      .filter(f => DateUtility.isBetween(f.date.get, startDateJoda, endDateJoda)
        && (!f.dCaricamentoFromLocalFile.isAfter(ghigliottinaJoda)))

    // Nel caso di SBG, filtriamo le misure per trattamento
    val filterRDD = filterRDDWithTreatmentSBG(flowRDD, rcuTreatment)

    // Processo di rimozione delle misure duplicate
    val flowRDDWithoutDuplicates = FlowController.removeDuplicateFlows(filterRDD).persist(StorageLevel.MEMORY_AND_DISK)

    var measureInclusionFiltered: RDD[Flow] = Environment.getSpark.sparkContext.emptyRDD

    // Processo di inclusione di alcune misure, se attivo
    if (inclusionFilters.isEmpty) measureInclusionFiltered = flowRDDWithoutDuplicates
    else inclusionFilters.foreach(inclusionFilter => measureInclusionFiltered = measureInclusionFiltered.union(inclusionFilter.filter(flowRDDWithoutDuplicates)).coalesce(flowRDDWithoutDuplicates.getNumPartitions))
    flowRDDWithoutDuplicates.unpersist()

    // Processo di esclusione standard di alcune misure, se attiva
    val measuresExclusionFiltered = if (softExclusionFilterController.isEnabled)
      softExclusionFilterController.excludeFlows(measureInclusionFiltered.distinct())
    else measureInclusionFiltered

    // Processo di esclusione definitiva di alcune misure, se attiva
    val measuresStrongExclusionFiltered = if (strongExclusionFilterController.isEnabled)
      strongExclusionFilterController.excludeFlows(measuresExclusionFiltered.distinct())
    else measuresExclusionFiltered

    if (!softExclusionFilterController.isEnabled && !strongExclusionFilterController.isEnabled)
      measuresStrongExclusionFiltered.distinct()
    else measuresStrongExclusionFiltered
  }

  val spark = Environment.getSpark.sqlContext

  import spark.implicits._

  /** Legge le misure di tipo RML nell'intervallo di date considerato */
  def getRmlMeasures(startDate: String, endDate: String, getTreatment: Boolean = true, rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG] = Environment.getSpark.emptyDataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    getMeasures(startDate, endDate, rmlDaoList, getTreatment, rcuTreatment)
  }

  /** Legge tutte le rimanenti misure nell'intervallo di date considerato */
  def getAllOtherMeasures(startDate: String, endDate: String, getTreatment: Boolean = true, rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG] = Environment.getSpark.emptyDataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    getMeasures(startDate, endDate, listDAO, getTreatment, rcuTreatment)
  }

  /** Legge le misure di assegnazione del trattamento nell'intervallo di date considerato */
  def getTreatmentMeasures(startDate: String, endDate: String, getTreatment: Boolean = true, rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG] = Environment.getSpark.emptyDataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    getMeasures(startDate, endDate, treatmentFlowDAOList, getTreatment, rcuTreatment).filter(f => {
      !f.isInstanceOf[Rgl] ||
        (f.isInstanceOf[Rgl] && f.motivation != Some(6))
    })
  }

  /** Legge le misure con motivazione 6 nell'intervallo di date considerato */
  def getMot6Measures(startDate: String, endDate: String, getTreatment: Boolean = true, rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG] = Environment.getSpark.emptyDataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    getMeasures(startDate, endDate, mot6DaoList, getTreatment, rcuTreatment).filter(f => {
      !f.isInstanceOf[Rgl] ||
        (f.isInstanceOf[Rgl] && f.motivation == Some(6))
    })
  }

  /** Legge le misure di intervento tecnico/cambio misuratore nell'intervallo di date considerato */
  def getIm1IgmgMeasures(startDate: String, endDate: String, getTreatment: Boolean = true, rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG] = Environment.getSpark.emptyDataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    getMeasures(startDate, endDate, igmgList, getTreatment, rcuTreatment).filter(Im1IgmgCorrectionController.im1IgmgReadFilter)
  }

  //override only sbg
  def filterRDDWithTreatmentSBG(rdd: RDD[Flow], rcuTreatment: Dataset[PdrWithMonthTreatmentYSBG]): RDD[Flow] = {
    rdd
  }

  /** Seleziona l'ultima RML inviata a parità di PdR e data della misura.
   * Queste vengono poi divise in base alla motivazione, per essere inserite nei rispettivi insiemi di appartenenza
   * (misure di trattamento e misure con motivazione 6)
   *
   * (AU-572, AU-568) */
  def getLatestRml(measures: RDD[Flow]): (RDD[Flow], RDD[Flow]) = {
    val latestRmlMeasures = measures
      .keyBy(f => (f.pdr, f.date))
      .groupByKey
      .values
      .map(flows => flows.toList.sorted(Flow.orderingSameDayFlows).last)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val treatmentRml = latestRmlMeasures.filter(_.motivation != Some(6))
    val mot6Rml = latestRmlMeasures.filter(_.motivation == Some(6))

    (treatmentRml, mot6Rml)
  }
}

object FlowController {
  def isDuplicateFilterEnabled: Boolean = Environment.isDuplicateFilterEnabled.equals("true")

  def isDuplicateFilterGroupByFilePathEnabled: Boolean = Environment.isDuplicateFilterGroupByFilePathEnabled.equals("true")

  def isDuplicateFilterGroupByTimestampEnabled: Boolean = Environment.isDuplicateFilterGroupByTimestampEnabled.equals("true")

  def isDuplicateFilterGroupByFileNameEnabled: Boolean = Environment.isDuplicateFilterGroupByFileNameEnabled.equals("true")

  def isAggSession: Boolean = Environment.getSession.contains("AGG")

  val orderingFlowsByDateTime: Ordering[DateTime] = new Ordering[DateTime] {
    override def compare(x: DateTime, y: DateTime): Int = x.compareTo(y)
  }

  /** Lista dei flussi da escludere per il caso 2 */
  val excludedFlowsFromCase2 =
    List(classOf[A01],
      classOf[A02],
      classOf[A40],
      classOf[D01],
      classOf[Im1],
      classOf[M01],
      classOf[R01],
      classOf[R40],
      classOf[S02],
      classOf[S40],
      classOf[Sm1],
      classOf[Sm2],
      classOf[V01],
      classOf[V02])

  /**
   * Removes duplicate measures, if any. The process is quite complex, there are three different cases applied in sequence.
   * @param measures an rdd of measures
   * @return <p>the rdd of measures filtered: we check four cases to determine which measures are correct.</p>
   *         <p>Measures where `localFile` and `date` are both empty are kept and filtering is delegated to the next controllers</p>
   */
  def removeDuplicateFlows(measures: RDD[Flow]): RDD[Flow] = {
    val getIsAggSession = isAggSession
    if (isDuplicateFilterEnabled) {

      val measuresGroupByFilePath = if (!isDuplicateFilterGroupByFilePathEnabled) measures
        .keyBy(f => (f.pdr, f.service, f.date))
        .groupByKey()
      else

      /**
       * Case 1:
       * group by pdr, service, date and localFile (path+file name)
       * If there are at least 2 measures, we check the equality of:
       * measure/converted/serialNumberMis/serialNumberConv/motivation/pivaUdd/cauIntMis/cauIntCorr (last two for Im1/Igmg only)
       * If any of these fields are different, the measured are discarded.
       * (We find duplicated PDRs inside the same file)
       *
       * If there's only one measure, that measure passes through.
       */
        measures
          .keyBy(f => (f.pdr, f.service, f.date))
          .groupByKey()
          .map({ case ((pdr, service, date), flows) =>
            val result = flows.groupBy(_.localFile)
              .filter({ case (localFile, flows) =>
                service match {
                  case "IGMG" | "IM1" => (localFile.isDefined && date.isDefined && (flows.size < 2 || areAllMeasureEqualsForIm1Igmg(flows))) || //either there is only one measure or they are all the same
                    localFile.isEmpty || date.isEmpty //otherwise, if fields are null delegate to next controllers
                  case _ => (localFile.isDefined && date.isDefined && (flows.size < 2 || areAllMeasureEquals(flows))) || //either there is only one measure or they are all the same
                    localFile.isEmpty || date.isEmpty //otherwise, if fields are null delegate to next controllers
                }
              }).values.reduceOption(_.++(_))

            ((pdr, service, date), result)
          })
          .filter({ case ((pdr, service, date), flows) => flows.isDefined })
          .map({case ((pdr, service, date), flows) => ((pdr, service, date), flows.get)})

      val measuresGroupByTimestamp = if (!isDuplicateFilterGroupByTimestampEnabled)
        measuresGroupByFilePath
      else

      /**
       * Case 2:
       * group by pdr, service, date and timestamp
       * First, in AGG case, we exclude the check for flows whose service is among `excludedFlowsFromCase2`. To do so, we use `isAggSession` flag.
       * Then, if there are at least 2 measures, we check the equality of:
       * file name
       * If any of these fields are different, the measured are discarded.
       *
       * If there's only one measure, that measure passes through.
       * */
        measuresGroupByFilePath
          .map({ case ((pdr, service, date), flows) =>
            val result = flows.groupBy(_.timestampLocalFile)
              .filter({ case (timestampLocalFile, flows) =>
                (getIsAggSession && excludedFlowsFromCase2.exists(_.isInstance(flows.head))) ||
                  ((date.isDefined && (flows.size < 2 || areAllFileNamesEquals(flows))) || //either there is only one measure or they are all the same
                    date.isEmpty) //otherwise, if fields are null delegate to next controllers
              }).values.reduceOption(_.++(_))

            ((pdr, service, date), result)
          })
          .filter({ case ((pdr, service, date), flows) => flows.isDefined })
          .map({case ((pdr, service, date), flows) => ((pdr, service, date), flows.get)})

      val measuresGroupByFileName = if (!isDuplicateFilterGroupByFileNameEnabled) measuresGroupByTimestamp
      else

      /**
       * Case 3:
       * group by pdr, service, date and file name
       * If there are at least 2 measures, we discard measures for which pivaUtente is different from pivaUddFromLocalPath
       * If there's only one measure, that measure passes through.
       * */
        measuresGroupByTimestamp
          .map({ case ((pdr, service, date), flows) =>
            val result = flows.groupBy(_.fileName)
              .map({ case (fileName, flows) =>
                if (areAllLocalFilesEquals(flows)) flows
                else flows.filter(f => f.pivaUtente.getOrElse("") == f.pivaUddFromLocalPath.getOrElse("-1"))
              }).reduceOption(_.++(_))
            ((pdr, service, date), result)

          })
          .filter({ case ((pdr, service, date), flows) => flows.isDefined })
          .map({ case ((pdr, service, date), flows) => ((pdr, service, date), flows.get) })

      /**
       * Case 4:
       * group by pdr, service, date, localFile (path+file name)
       * After having checked cases 1-2-3, we select the most recently uploaded measure(s).
       * */
      measuresGroupByFileName
        .map({ case ((pdr, service, date), flows) =>
          flows.groupBy(_.localFile)
            .map({ case (localFile, flows) =>
              service match {
                case "IGMG" | "IM1" =>
                  if (flows.size < 2 || !areAllMeasureEqualsExceptDCaricamentoForIm1Igmg(flows)) flows
                  else Iterable(flows.maxBy(_.dataCaricamento.getOrElse(DateTime.parse("01/01/1900", DateTimeFormat.forPattern("dd/MM/yyyy"))))(orderingFlowsByDateTime))
                case _ =>
                  if (flows.size < 2 || !areAllMeasureEqualsExceptDCaricamento(flows)) flows
                  else Iterable(flows.maxBy(_.dataCaricamento.getOrElse(DateTime.parse("01/01/1900", DateTimeFormat.forPattern("dd/MM/yyyy"))))(orderingFlowsByDateTime))
              }
            }).reduceOption(_.++(_))
        })
        .filter(_.isDefined)
        .flatMap(f => f.get)
    }
    else {
      measures
    }
  }

  /**
   * @param flows an iterable of measures
   * @return <p>true iff for all <i>i, j</i> with <i>i>j</i>, `flows[i]` and `flows[j]` have
   *         the same `measure`, `converted`,
   *         `serialNumberMis`, `serialNumberConv`, `pivaUtente` and `motivation` (if any).</p>
   *         <p><b>NB. None values are equals!</b></p>
   */
  @tailrec
  final def areAllMeasureEquals(flows: Iterable[Flow]): Boolean = {
    if (flows.size < 2) true //base case
    else {
      val flow = flows.head
      val flowPrime = flows.tail.head
      //inductive step
      if (
        flow.measure.equals(flowPrime.measure) && flow.converted.equals(flowPrime.converted) &&
          flow.serialNumberMis.equals(flowPrime.serialNumberMis) && flow.serialNumberConv.equals(flowPrime.serialNumberConv) &&
          flow.motivation.equals(flowPrime.motivation) &&
          flow.pivaUtente.getOrElse(flow.pivaUddFromLocalPath).equals(flowPrime.pivaUtente.getOrElse(flowPrime.pivaUddFromLocalPath))
      )
        areAllMeasureEquals(flows.tail)
      //stop condition
      else false
    }
  }

  /**
   * @param flows an iterable of measures of type [[Im1Igmg]]
   * @return <p>true iff for all <i>i, j</i> with <i>i>j</i>, `flows[i]` and `flows[j]` have
   *         the same `measure`, `converted`,
   *         `serialNumberMis`, `serialNumberConv`, `pivaUtente` and `motivation` (if any).</p>
   *         <p><b>NB. None values are equals!</b></p>
   */
  @tailrec
  final def areAllMeasureEqualsForIm1Igmg(flows: Iterable[Flow]): Boolean = {
    if (flows.size < 2) true //base case
    else {
      val flowPre = flows.head.asInstanceOf[Im1Igmg].pre
      val flowPrimePre = flows.tail.head.asInstanceOf[Im1Igmg].pre
      val flowPost = flows.head.asInstanceOf[Im1Igmg].post
      val flowPrimePost = flows.tail.head.asInstanceOf[Im1Igmg].post

      if (
        flowPre.measure.equals(flowPrimePre.measure) &&
          flowPre.converted.equals(flowPrimePre.converted) &&
          flowPre.serialNumberMis.equals(flowPrimePre.serialNumberMis) &&
          flowPre.serialNumberConv.equals(flowPrimePre.serialNumberConv) &&
          flowPre.motivation.equals(flowPrimePre.motivation) &&
          flowPre.pivaUtente.getOrElse(flowPre.pivaUddFromLocalPath).equals(flowPrimePre.pivaUtente.getOrElse(flowPrimePre.pivaUddFromLocalPath)) &&
          flowPre.cau_int_mis.equals(flowPrimePre.cau_int_mis) &&
          flowPre.cau_int_cor.equals(flowPrimePre.cau_int_cor) &&

          flowPost.measure.equals(flowPrimePost.measure) &&
          flowPost.converted.equals(flowPrimePost.converted) &&
          flowPost.serialNumberMis.equals(flowPrimePost.serialNumberMis) &&
          flowPost.serialNumberConv.equals(flowPrimePost.serialNumberConv) &&
          flowPost.motivation.equals(flowPrimePost.motivation) &&
          flowPost.pivaUtente.getOrElse(flowPost.pivaUddFromLocalPath).equals(flowPrimePost.pivaUtente.getOrElse(flowPrimePost.pivaUddFromLocalPath)) &&
          flowPost.cau_int_mis.equals(flowPrimePost.cau_int_mis) &&
          flowPost.cau_int_cor.equals(flowPrimePost.cau_int_cor)
      )
        areAllMeasureEqualsForIm1Igmg(flows.tail)
      else false
    }
  }

  /**
   * @param flows an iterable of measures
   * @return <p>true iff for all <i>i, j</i> with <i>i>j</i>, `flows[i]` and `flows[j]` have
   *         the same `fileName`.</p>
   */
  @tailrec
  final def areAllFileNamesEquals(flows: Iterable[Flow]): Boolean = {
    if (flows.size < 2) true //base case
    else {
      val flow = flows.head
      val flowPrime = flows.tail.head
      //inductive step
      if (flow.fileName.equals(flowPrime.fileName)) areAllFileNamesEquals(flows.tail)
      //stop condition
      else false
    }
  }

  /**
   * @param flows an iterable of measures
   * @return <p>true iff for all <i>i, j</i> with <i>i>j</i>, `flows[i]` and `flows[j]` have
   *         the same `localFile`. Since we group by `fileName`, we directly check the equality between `localFiles`
   *         (path+file name).</p>
   */
  @tailrec
  final def areAllLocalFilesEquals(flows: Iterable[Flow]): Boolean = {
    if (flows.size < 2) true //base case
    else {
      val flow = flows.head
      val flowPrime = flows.tail.head
      //inductive step
      if (flow.localFile.getOrElse("").equals(flowPrime.localFile.getOrElse(""))) areAllLocalFilesEquals(flows.tail)
      //stop condition
      else false
    }
  }

  /**
   * @param flows an iterable of measures
   * @return <p>true iff for all <i>i, j</i> with <i>i>j</i>, `flows[i]` and `flows[j]` have
   *         the same `measure`, `converted`,
   *         `serialNumberMis`, `serialNumberConv`, `pivaDistr` and `motivation` (if any).</p>
   *         <p><b>NB. None values are equals!</b></p>
   */
  @tailrec
  final def areAllMeasureEqualsExceptDCaricamento(flows: Iterable[Flow]): Boolean = {
    if (flows.size < 2) true //base case
    else {
      val flow = flows.head
      val flowPrime = flows.tail.head
      //inductive step
      if (
        flow.measure.equals(flowPrime.measure) &&
          flow.converted.equals(flowPrime.converted) &&
          flow.serialNumberMis.equals(flowPrime.serialNumberMis) &&
          flow.serialNumberConv.equals(flowPrime.serialNumberConv) &&
          //flow.dataCaricamento.equals(flowPrime.dataCaricamento) && its a bug: d_caricamento is not part of this check since we must remove flows with everything but d_caricamento equals to each others
          flow.pivaDistr.equals(flowPrime.pivaDistr) &&
          flow.isValid.equals(flowPrime.isValid) &&
          flow.outcome.equals(flowPrime.outcome) &&
          flow.readType.equals(flowPrime.readType) &&
          flow.motivation.equals(flowPrime.motivation) &&
          flow.ammissibilita.equals(flowPrime.ammissibilita)
      )
        areAllMeasureEqualsExceptDCaricamento(flows.tail)
      //stop condition
      else false
    }
  }

  /**
   * @param flows an iterable of measures
   * @return <p>true iff for all <i>i, j</i> with <i>i>j</i>, `flows[i]` and `flows[j]` have
   *         the same `measure`, `converted`,
   *         `serialNumberMis`, `serialNumberConv`, `pivaDistr` and `motivation` (if any).</p>
   *         <p><b>NB. None values are equals!</b></p>
   */
  @tailrec
  final def areAllMeasureEqualsExceptDCaricamentoForIm1Igmg(flows: Iterable[Flow]): Boolean = {
    if (flows.size < 2) true //base case
    else {
      val flowPre = flows.head.asInstanceOf[Im1Igmg].pre
      val flowPrimePre = flows.tail.head.asInstanceOf[Im1Igmg].pre
      val flowPost = flows.head.asInstanceOf[Im1Igmg].post
      val flowPrimePost = flows.tail.head.asInstanceOf[Im1Igmg].post
      //inductive step
      if (
        flowPre.measure.equals(flowPrimePre.measure) &&
          flowPre.converted.equals(flowPrimePre.converted) &&
          flowPre.serialNumberMis.equals(flowPrimePre.serialNumberMis) &&
          flowPre.serialNumberConv.equals(flowPrimePre.serialNumberConv) &&
          flowPre.pivaDistr.equals(flowPrimePre.pivaDistr) &&
          flowPre.isValid.equals(flowPrimePre.isValid) &&
          flowPre.outcome.equals(flowPrimePre.outcome) &&
          flowPre.readType.equals(flowPrimePre.readType) &&
          flowPre.motivation.equals(flowPrimePre.motivation) &&
          flowPre.ammissibilita.equals(flowPrimePre.ammissibilita) &&

          flowPost.measure.equals(flowPrimePost.measure) &&
          flowPost.converted.equals(flowPrimePost.converted) &&
          flowPost.serialNumberMis.equals(flowPrimePost.serialNumberMis) &&
          flowPost.serialNumberConv.equals(flowPrimePost.serialNumberConv) &&
          flowPost.pivaDistr.equals(flowPrimePost.pivaDistr) &&
          flowPost.isValid.equals(flowPrimePost.isValid) &&
          flowPost.outcome.equals(flowPrimePost.outcome) &&
          flowPost.readType.equals(flowPrimePost.readType) &&
          flowPost.motivation.equals(flowPrimePost.motivation) &&
          flowPost.ammissibilita.equals(flowPrimePost.ammissibilita)
      )
        areAllMeasureEqualsExceptDCaricamentoForIm1Igmg(flows.tail)
      //stop condition
      else false
    }
  }
}