package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.dao.measure.MeasureDAO.parseDateToOption
import it.eng.au.calcoloIndennizzi.model.measure.Tgl
import it.eng.au.calcoloIndennizzi.model.measure.Tgl.orderingFlowsByDateTime
import it.eng.au.calcoloIndennizzi.schema.cig.PdrGSettimoSchema
import it.eng.au.calcoloIndennizzi.schema.measure.TglSchema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.calcoloIndennizzi.utility.constants.Constants._
import it.eng.au.indennizziMisureGasCommon.utility.dataframe.DataFrameUtils.isNotNullNorEmpty
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

/** Implementa una serie di operazioni per estrarre l'insieme delle TGL ammissibili che concorrono al calcolo degli indennizzi. */
object TglController extends Serializable {
  val yearFromPath = "year_from_path"
  val monthDayFromPath = "month_day_from_path"
  val dateFromPath = "date_from_path"
  val hasRaccoltaP = "has_raccolta_P"
  val isTglOM1 = "is_tgl_om1"
  val isTglOM2 = "is_tgl_om2"
  val isTglOM3 = "is_tgl_om3"

  def isDuplicateFilterEnabled: Boolean = Properties.isDuplicateFilterEnabled
  def isDuplicateFilterGroupByFilePathEnabled: Boolean = Properties.isDuplicateFilterGroupByFilePathEnabled
  def isDuplicateFilterGroupByTimestampEnabled: Boolean = Properties.isDuplicateFilterGroupByTimestampEnabled
  def isDuplicateFilterGroupByFileNameEnabled: Boolean = Properties.isDuplicateFilterGroupByFileNameEnabled

  /**
   * Aggiunge al dataframe [[df]] le informazioni sulle TGL relative agli Obiettivi Minini 1,2,3. In particolare,
   *  1. L'OM1 richiede che la TGL sia ammissibile e trasmessa entro il giorni di soglia del mese;
   *  1. L'OM2 richiede che il numero di TGL ammissibili, trasmesse nei tempi ed effettive sia in un certo range (valore di default [100, 100])
   *  1. L'OM3 richiede che il numero di TGL ammissibili, trasmesse nei tempi ed effettive sia in un certo range, estremo destro eslcuso (valore di default [30, 100[)
   * @param df dataframe delle TGL ammissibili e trasmesse entro il giorno di soglia
   * @return lo stesso dataframe [[df]] con le info sugli OM aggiunti
   */
  def getInfo(df: DataFrame): DataFrame = {
    val daysInMonth = Properties.getDaysInMonth.toInt
    val om2LowerBound = Properties.getOM2LowerBound.toFloat / 100
    val om2UpperBound = Properties.getOM2UpperBound.toFloat / 100
    val om3LowerBound = Properties.getOM3LowerBound.toFloat / 100
    val om3UpperBound = Properties.getOM3UpperBound.toFloat / 100

    df
      .withColumn(isTglOM1, lit(true))
      .withColumn(isTglOM2, when(col(PdrGSettimoSchema.count_tgl_effettive).between(om2LowerBound * daysInMonth, om2UpperBound * daysInMonth), true).otherwise(false))
      .withColumn(isTglOM3, when(col(PdrGSettimoSchema.count_tgl_effettive).between(om3LowerBound * daysInMonth, om3UpperBound * daysInMonth) &&
        col(PdrGSettimoSchema.count_tgl_effettive) =!= om3UpperBound * daysInMonth, true).otherwise(false))
  }

  /**
   * Estrae l'insieme delle TGL ammissibili e trasmesse entro il giorno di soglia.
   * @param df dataframe grezzo delle TGL
   * @return lo stesso dataframe [[df]] contenente soltanto le TGL ammissibili e trasmesse entro il giorno di soglia
   */
  def getTgl(df: DataFrame): DataFrame = {
    val sqlContext = Environment.sqlContext
    import sqlContext.implicits._

    val tgl = modifyColumns(df)
    val tglFiltered = filterAndSelect(tgl)
    val tglRdd = tglFiltered.rdd.map(mapFunction)
    val tglWithoutDuplicateFlows = removeDuplicateFlows(tglRdd)
    val filteredTgl = ExclusionFilterController.excludeTgls(tglWithoutDuplicateFlows)
    val priorityTgl = getPriorityMeasures(filteredTgl)

    priorityTgl
      .map(tgl =>
        (tgl.pdr,
          tgl.localFile,
          tgl.readType.get.toString
        ))
      .toDF(
        TglSchema.cod_pdr,
        TglSchema.local_file,
        TglSchema.tipo_lettura
      )
  }

  /** Estrae e mappa dal dataframe grezzo delle TGL i campi che ci interessano. */
  def modifyColumns(df: DataFrame): DataFrame = {
    df
      .withColumn(TglSchema.tipo_lettura, substring(trim(col(TglSchema.tipo_lettura)), 1, 1))
      .withColumn(TglSchema.data_comp, from_unixtime(unix_timestamp(col(TglSchema.data_comp), DATA_COMP_FORMAT), DATE_FORMAT))
      .withColumn(TglSchema.mese_comp, from_unixtime(unix_timestamp(col(TglSchema.mese_comp), MONTH_YEAR_FORMAT), YEAR_MONTH_FORMAT))
      .withColumn(TglSchema.d_caricamento, col(TglSchema.d_caricamento).cast(TimestampType))
      .withColumn(TglSchema.let_tot_prel, col(TglSchema.let_tot_prel).cast(DoubleType))
      .withColumn(TglSchema.let_tot_conv, col(TglSchema.let_tot_conv).cast(DoubleType))
      .withColumn(TglSchema.matr_mis, coalesce(col(TglSchema.matr_mis_giornaliere), col(TglSchema.matr_mis)))
      .withColumn(TglSchema.matr_conv, coalesce(col(TglSchema.matr_conv_giornaliere), col(TglSchema.matr_conv)))
  }

  /** Filtra il dataframe delle TGL selezionando soltanto le TGL ammissibili e trasmesse entro il giorno di soglia. */
  def filterAndSelect(df: DataFrame): DataFrame = {
    val yearMonth = Properties.getYearMonth
    val yearMonthFormatter = DateTimeFormat.forPattern(YEAR_MONTH_FORMAT)
    val dayOfMonthThreshold = Properties.getTglDayOfMonthThreshold.toInt

    // yyyyMM mese di calcolo, d giorno selezionato come ghigliottina
    // thresholdDate è d/(MM+1)/yyyy
    // in modo tale da verificare dateFromPath <= thresholdDate
    val thresholdDate = DateTime.parse(yearMonth, yearMonthFormatter).plusMonths(1).withDayOfMonth(dayOfMonthThreshold).toString(DATE_FORMAT)

    val window = Window.partitionBy(col(TglSchema.cod_pdr), col(TglSchema.local_file))

    df
      .where(col(TglSchema.mese_comp) === yearMonth)
      .withColumn(yearFromPath, split(col("local_file"), "/").getItem(6))
      .withColumn(monthDayFromPath, split(col("local_file"), "/").getItem(7))
      .withColumn(dateFromPath, concat(col(yearFromPath), col(monthDayFromPath)))
      .withColumn(dateFromPath, from_unixtime(unix_timestamp(col(dateFromPath), YEAR_MONTH_DAY_FORMAT), DATE_FORMAT).cast(DateType))
      .where(col(dateFromPath) <= lit(thresholdDate).cast(DateType))
      .withColumn(TglSchema.d_caricamento, col(TglSchema.d_caricamento).cast(StringType))
      .where(col(TglSchema.tipo_lettura).isin(EFFETTIVA, STIMATA))
      .where(isNotNullNorEmpty(col(TglSchema.cod_pdr)))
      .where(isNotNullNorEmpty(col(TglSchema.piva_distr)))
      .where(isNotNullNorEmpty(col(TglSchema.data_comp)))
      .where((col(TglSchema.ammissibilita).isNotNull && col(TglSchema.ammissibilita) =!= BLOCCANTE) ||
        (col(TglSchema.ammissibilita).isNull && col(TglSchema.val_dato_mens) === VAL_DATO_SI))
      .withColumn(hasRaccoltaP, max(col(TglSchema.raccolta) === lit(RACCOLTA_P)).over(window))
      .where(col(hasRaccoltaP))
      .select(
        TglSchema.cod_pdr,
        TglSchema.data_comp,
        TglSchema.let_tot_prel,
        TglSchema.let_tot_conv,
        TglSchema.matr_mis,
        TglSchema.matr_conv,
        TglSchema.local_file,
        TglSchema.piva_utente,
        TglSchema.piva_distr,
        TglSchema.d_caricamento,
        TglSchema.tipo_lettura,
        TglSchema.val_dato_mens,
        TglSchema.ammissibilita
      )
  }

  /** Mappa i campi del dataframe delle TGL in campi della case class [[Tgl]], creando così un RDD.*/
  def mapFunction(row: Row): Tgl = {
    Tgl(
      pdr = row.getAs(TglSchema.cod_pdr).toString,
      date = parseDateToOption(row.getAs[String](TglSchema.data_comp)),
      measure = Try(Option(row.getAs[String](TglSchema.let_tot_prel)).map(_.toDouble)).getOrElse(None),
      converted = Try(Option(row.getAs[String](TglSchema.let_tot_conv)).map(_.toDouble)).getOrElse(None),
      serialNumberMis = Option(row.getAs[String](TglSchema.matr_mis)),
      serialNumberConv = Option(row.getAs[String](TglSchema.matr_conv)),
      localFile = Option(row.getAs[String](TglSchema.local_file)),
      pivaUtente = Option(row.getAs[String](TglSchema.piva_utente)),
      pivaDistr = Option(row.getAs[String](TglSchema.piva_distr)),
      dataCaricamento = parseDateToOption(row.getAs[String](TglSchema.d_caricamento)),
      readType = Option(row.getAs[Char](TglSchema.tipo_lettura)),
      isValid = Option(row.getAs[String](TglSchema.val_dato_mens)),
      ammissibilita = Option(row.getAs[String](TglSchema.ammissibilita))
    )
  }

  /** Aggrega le TGL e calcola il numero delle TGL effettive e stimate. */
  def aggregateTgl(df: DataFrame): DataFrame = {
    val daysInMonth = Properties.getDaysInMonth.toInt

    def concatCollectSetUDF: UserDefinedFunction = udf((list: mutable.WrappedArray[String], sep: String) => list.mkString(sep))

    df
      .groupBy(col(TglSchema.cod_pdr))
      .agg(
        collect_set(col(TglSchema.local_file)).alias(TglSchema.local_file),
        sum(when(col(TglSchema.tipo_lettura) === "E", 1).otherwise(0)).alias(PdrGSettimoSchema.count_tgl_effettive)
      )
      .withColumn(PdrGSettimoSchema.count_tgl_stimate, lit(daysInMonth).cast(IntegerType) - col(PdrGSettimoSchema.count_tgl_effettive))
      .withColumn(TglSchema.local_file, concatCollectSetUDF(col(TglSchema.local_file), lit(",")))
      .select(
        TglSchema.cod_pdr,
        TglSchema.local_file,
        PdrGSettimoSchema.count_tgl_effettive,
        PdrGSettimoSchema.count_tgl_stimate
      )
  }

  /** Esegue il processo di priorità per ottenere un'unica TGL per ogni PdR e giorno. */
  def getPriorityMeasures(measures: RDD[Tgl]): RDD[Tgl] = {
    measures
      .keyBy(f => (f.pdr, f.date))
      .groupByKey().values
      .map(flows => flows.toList.max(Tgl.orderingSameDayFlows))
  }

  /** Esegue il processo di rimozione dei flussi duplicati (presente anche in AGG/SBG/CDP, consultare i documenti tecnici per maggiori dettagli). */
  def removeDuplicateFlows(measures: RDD[Tgl]): RDD[Tgl] = {
    if (isDuplicateFilterEnabled) {
      val measuresGroupByFilePath = if (!isDuplicateFilterGroupByFilePathEnabled) measures
        .keyBy(f => (f.pdr, f.date))
        .groupByKey()
      else {

        /**
         * Case 1:
         * group by pdr, date and localFile (path+file name)
         * If there are at least 2 measures, we check the equality of:
         * measure/converted/serialNumberMis/serialNumberConv/pivaUdd
         * If any of these fields are different, the measured are discarded.
         * (We find duplicated PDRs inside the same file)
         *
         * If there's only one measure, that measure passes through.
         */
        measures
          .keyBy(f => (f.pdr, f.date))
          .groupByKey()
          .map({ case ((pdr, date), flows) =>
            val result = flows.groupBy(_.localFile)
              .filter({ case (localFile, flows) =>
                (localFile.isDefined && date.isDefined && (flows.size < 2 || areAllMeasureEquals(flows))) || //either there is only one measure or they are all the same
                  localFile.isEmpty || date.isEmpty //otherwise, if fields are null delegate to next controllers
              }).values.reduceOption(_.++(_))

            ((pdr, date), result)
          })
          .filter({ case ((pdr, date), flows) => flows.isDefined })
          .map({ case ((pdr, date), flows) => ((pdr, date), flows.get) })
      }

      val measuresGroupByTimestamp = if (!isDuplicateFilterGroupByTimestampEnabled)
        measuresGroupByFilePath
      else

      /**
       * Case 2:
       * group by pdr, date and timestamp
       * If there are at least 2 measures, we check the equality of:
       * file name
       * If any of these fields are different, the measured are discarded.
       *
       * If there's only one measure, that measure passes through.
       * */
        measuresGroupByFilePath
          .map({ case ((pdr, date), flows) =>
            val result = flows.groupBy(_.timestampLocalFile)
              .filter({ case (timestampLocalFile, flows) =>
                (date.isDefined && (flows.size < 2 || areAllFileNamesEquals(flows))) || //either there is only one measure or they are all the same
                  date.isEmpty //otherwise, if fields are null delegate to next controllers
              }).values.reduceOption(_.++(_))

            ((pdr, date), result)
          })
          .filter({ case ((pdr, date), flows) => flows.isDefined })
          .map({ case ((pdr, date), flows) => ((pdr, date), flows.get) })

      val measuresGroupByFileName = if (!isDuplicateFilterGroupByFileNameEnabled) measuresGroupByTimestamp
      else

      /**
       * Case 3:
       * group by pdr, date and file name
       * If there are at least 2 measures, we discard measures for which pivaUtente is different from pivaUddFromLocalPath
       * If there's only one measure, that measure passes through.
       * */
        measuresGroupByTimestamp
          .map({ case ((pdr, date), flows) =>
            val result = flows.groupBy(_.fileName)
              .map({ case (fileName, flows) =>
                if (areAllLocalFilesEquals(flows)) flows
                else flows.filter(f => f.pivaUtente.getOrElse("") == f.pivaUddFromLocalPath.getOrElse("-1"))
              }).reduceOption(_.++(_))
            ((pdr, date), result)

          })
          .filter({ case ((pdr, date), flows) => flows.isDefined })
          .map({ case ((pdr, date), flows) => ((pdr, date), flows.get) })

      /**
       * Case 4:
       * group by pdr, date, localFile (path+file name)
       * After having checked cases 1-2-3, we select the most recently uploaded measure(s).
       * */
      measuresGroupByFileName
        .map({ case ((pdr, date), flows) =>
          flows.groupBy(_.localFile)
            .map({ case (localFile, flows) =>
              if (flows.size < 2 || !areAllMeasureEqualsExceptDCaricamento(flows)) flows
              else Iterable(flows.maxBy(_.dataCaricamento.getOrElse(DateTime.parse("01/01/1900", DateTimeFormat.forPattern("dd/MM/yyyy"))))(orderingFlowsByDateTime))
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
   *         `serialNumberMis`, `serialNumberConv`, `pivaUtente`.</p>
   *         <p><b>NB. None values are equals!</b></p>
   */
  @tailrec
  final def areAllMeasureEquals(flows: Iterable[Tgl]): Boolean = {
    if (flows.size < 2) true //base case
    else {
      val flow = flows.head
      val flowPrime = flows.tail.head
      //inductive step
      if (
        flow.measure.equals(flowPrime.measure) && flow.converted.equals(flowPrime.converted) &&
          flow.serialNumberMis.equals(flowPrime.serialNumberMis) && flow.serialNumberConv.equals(flowPrime.serialNumberConv) &&
          flow.pivaUtente.getOrElse(flow.pivaUddFromLocalPath).equals(flowPrime.pivaUtente.getOrElse(flowPrime.pivaUddFromLocalPath))
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
  final def areAllFileNamesEquals(flows: Iterable[Tgl]): Boolean = {
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
  final def areAllLocalFilesEquals(flows: Iterable[Tgl]): Boolean = {
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
   *         `serialNumberMis`, `serialNumberConv`, `pivaDistr`, `isValid`, `readType` and `ammissibilita` .</p>
   *         <p><b>NB. None values are equals!</b></p>
   */
  @tailrec
  final def areAllMeasureEqualsExceptDCaricamento(flows: Iterable[Tgl]): Boolean = {
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
          flow.ammissibilita.equals(flowPrime.ammissibilita)
      )
        areAllMeasureEqualsExceptDCaricamento(flows.tail)
      //stop condition
      else false
    }
  }
}
