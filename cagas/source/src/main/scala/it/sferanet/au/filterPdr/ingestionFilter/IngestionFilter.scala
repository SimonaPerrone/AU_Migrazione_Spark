package it.sferanet.au.filterPdr.ingestionFilter

import it.sferanet.au.model.Flow
import it.sferanet.au.model.Flow.{getCauIntCor, getCauIntMis}
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.utilities.Constants.STANDARD_FORMAT_DATE
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD

import java.util.Date
import scala.annotation.tailrec

object IngestionFilter {
  val orderingFlowsByDateTime: Ordering[Date] = new Ordering[Date] {
    override def compare(x: Date, y: Date): Int = x.compareTo(y)
  }

  val excludedFlowsFromCase2 =
    List(classOf[A01],
      classOf[A02],
      classOf[A40],
      classOf[Im1Pre],
      classOf[Im1Post],
      classOf[M01],
      classOf[R01],
      classOf[R40],
      classOf[S02],
      classOf[S40],
      classOf[Sm1],
      classOf[V01],
      classOf[V02])

  def isDuplicateFilterEnabled: Boolean = Environment.isDuplicateMeasuresFilterEnabled.equals("true")

  def isDuplicateFilterGroupByFilePathEnabled: Boolean = Environment.isDuplicateFilterGroupByFilePathEnabled.equals("true")

  def isDuplicateFilterGroupByTimestampEnabled: Boolean = Environment.isDuplicateFilterGroupByTimestampEnabled.equals("true")

  def isDuplicateFilterGroupByFileNameEnabled: Boolean = Environment.isDuplicateFilterGroupByFileNameEnabled.equals("true")

  /**
   * @param measures an rdd of measures
   * @return <p>the rdd of measures filtered: we check four cases to determine which measures are correct.</p>
   *         <p>Measures where `localFile` and `date` are both empty are kept and filtering is delegated to the next controllers</p>
   */
  def removeDuplicateFlows(measures: RDD[Flow]): RDD[Flow] = {
    if (!isDuplicateFilterEnabled) {
      return measures
    }

    val measuresGroupByFilePath = if (!isDuplicateFilterGroupByFilePathEnabled) {
      measures
        .keyBy(f => (f.pdr, f.service, f.date))
        .groupByKey()
    } else {
      /**
       * Case 1:
       * group by pdr, service, date and localFile (path+file name)
       * If there are at least 2 measures, we check the equality of:
       * measure/converted/serialNumberMis/serialNumberConv/motivation/pivaUdd/cauIntMis/cauIntCor
       * If any of these fields are different, the measured are discarded.
       * (We find duplicated PDRs inside the same file)
       *
       * If there's only one measure, that measure passes through.
       */
      measures
        .keyBy(f => (f.pdr, f.service, f.date))
        .groupByKey()
        .map({
          case ((pdr, service, date), flows) => {
            val result = flows.groupBy(_.local_file)
              .filter({ case (localFile, flows) =>
                (localFile.isDefined && date.isDefined && (flows.size < 2 || areAllMeasureEquals(flows))) || //either there is only one measure or they are all the same
                  localFile.isEmpty || date.isEmpty //otherwise, if fields are null delegate to next controllers
              }).values.reduceOption(_.++(_))

            ((pdr, service, date), result)
          }
        })
        .filter({ case ((pdr, service, date), flows) => flows.isDefined })
        .map({ case ((pdr, service, date), flows) => ((pdr, service, date), flows.get) })
    }

    val measuresGroupByTimestamp = if (!isDuplicateFilterGroupByTimestampEnabled) {
      measuresGroupByFilePath
    } else {
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
        .map({
          case ((pdr, service, date), flows) => {
            val result = flows.groupBy(_.timestampLocalFile)
              .filter({ case (timestampLocalFile, flows) =>
                excludedFlowsFromCase2.exists(_.isInstance(flows.head)) ||
                  ((date.isDefined && (flows.size < 2 || areAllFileNamesEquals(flows))) || //either there is only one measure or they are all the same
                    date.isEmpty) //otherwise, if fields are null delegate to next controllers
              }).values.reduceOption(_.++(_))

            ((pdr, service, date), result)
          }
        })
        .filter({ case ((pdr, service, date), flows) => flows.isDefined })
        .map({ case ((pdr, service, date), flows) => ((pdr, service, date), flows.get) })
    }

    val measuresGroupByFileName = if (!isDuplicateFilterGroupByFileNameEnabled) {
      measuresGroupByTimestamp
    } else {
      /**
       * Case 3:
       * group by pdr, service, date and file name
       * If there are at least 2 measures, we discard measures for which pivaUtente is different from pivaUddFromLocalPath
       * If there's only one measure, that measure passes through.
       * */
      measuresGroupByTimestamp
        .map({
          case ((pdr, service, date), flows) => {
            val result = flows.groupBy(_.fileNameLocalFile)
              .map({ case (fileName, flows) =>
                if (areAllLocalFilesEquals(flows)) flows
                else flows.filter(f => f.pivaUtente.getOrElse("") == f.pivaUddFromLocalPath.getOrElse("-1"))
              }).reduceOption(_.++(_))

            ((pdr, service, date), result)
          }
        })
        .filter({ case ((pdr, service, date), flows) => flows.isDefined })
        .map({ case ((pdr, service, date), flows) => ((pdr, service, date), flows.get) })
    }

    /**
     * Case 4:
     * group by pdr, service, date, localFile (path+file name)
     * After having checked cases 1-2-3, we select the most recently uploaded measure(s).
     * */
    measuresGroupByFileName
      .map({
        case ((pdr, service, date), flows) => {
          flows.groupBy(_.local_file)
            .map({ case (localFile, flows) => {
              if (flows.size < 2 || !areAllMeasureEqualsExceptDCaricamento(flows)) {
                flows
              } else {
                Iterable(flows.maxBy(_.d_caricamento.getOrElse(STANDARD_FORMAT_DATE.parse("01/01/1900")))(orderingFlowsByDateTime))
              }
            }
            }).reduceOption(_.++(_))
        }
      })
      .filter(_.isDefined)
      .flatMap(f => f.get)

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
        flow.measure.equals(flowPrime.measure) &&
          flow.converted.equals(flowPrime.converted) &&
          flow.serialNumberMis.equals(flowPrime.serialNumberMis) &&
          flow.serialNumberConv.equals(flowPrime.serialNumberConv) &&
          flow.motivation.equals(flowPrime.motivation) &&
          flow.pivaUtente.getOrElse(flow.pivaUddFromLocalPath).equals(flowPrime.pivaUtente.getOrElse(flowPrime.pivaUddFromLocalPath)) &&
          getCauIntMis(flow).equals(getCauIntMis(flowPrime)) &&
          getCauIntCor(flow).equals(getCauIntCor(flowPrime))
      )
        areAllMeasureEquals(flows.tail)
      //stop condition
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
      if (flow.fileNameLocalFile.equals(flowPrime.fileNameLocalFile)) areAllFileNamesEquals(flows.tail)
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
      if (flow.local_file.getOrElse("").equals(flowPrime.local_file.getOrElse(""))) areAllLocalFilesEquals(flows.tail)
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
          flow.pivaDistr.equals(flowPrime.pivaDistr) &&
          flow.isValid.equals(flowPrime.isValid) &&
          flow.readType.equals(flowPrime.readType) &&
          flow.motivation.equals(flowPrime.motivation) &&
          flow.ammissibilita.equals(flowPrime.ammissibilita)
      )
        areAllMeasureEqualsExceptDCaricamento(flows.tail)
      //stop condition
      else false
    }
  }

}
