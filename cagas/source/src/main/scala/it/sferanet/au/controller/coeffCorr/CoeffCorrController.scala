package it.sferanet.au.controller.coeffCorr

import it.sferanet.au.controller.coeffCorr.CoeffCorrController._
import it.sferanet.au.model.{CoeffCorrMask, Flow, RcuGasMassivoTech}
import it.sferanet.au.utilities.DataframeUtils._
import it.sferanet.au.utilities.{Constants, DataframeUtils, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}

import java.util.Date

/**
 * Controllore per il calcolo della maschera dei coefficienti RCUGAS o IM1/IGMG da utilizzare per ogni misura
 */
class CoeffCorrController {

  private def isSamePdr: Column = {
    DataframeUtils.isSamePdr(f1.pdr, f2.pdr)
  }

  private def isLastPost: Column = {
    (isPost(col(f1.service)) && col(f2max).isNotNull && same(col(f1.date), col(f2max)))
  }


  /**
   * Condizione di Join per associare ad ogni misura IM1/IGMG
   * il coefficiente risultante, che deve essere lo stesso della misura originale in ingresso
   *
   * @return [[Column]] che rappresenta la condizione di Join
   */
  private def associateSameIm1IgmgMeasure: Column = {
    isMeterChangeMeasure(col(f1.service)) && same(col(f1.service), col(f2.service)) && same(col(f1.date), col(f2.date))
  }


  /**
   * Condizione di Join per associare ad ogni misura Diversa da IM1/IGMG PRE (condizione !isPre)
   * e dall'ultima (la più recente) IM1/IGMG POST (condizione !isLastPost)
   * tutti i possibili coefficienti PRE delle misure IM1PRE/IGMGPRE successive al flusso di misura stesso
   * (condizioni isPre(f2service) AND data(f1) < data(f2)
   *
   * @return [[Column]] che rappresenta la condizione di Join
   */
  private def associateMeasuresWithPre: Column = {
    !isPre(col(f1.service)) && !isLastPost &&
      isPre(col(f2.service)) && col(f1.date) < col(f2.date)
  }

  /**
   * Associa ad ogni misura che non appartiene ad un flusso di cambio misuratore, quindi NON IGMG/IM1 (condizione !isMeterChangeMeasure)
   * Il coefficiente di correzione dell'ultimo IM1POST/IGMGPOST, se la data della misura è successiva
   *
   * @return che rappresenta la condizione di Join
   */
  private def associateMeasuresWithPost: Column = {
    !isMeterChangeMeasure(col(f1.service)) && isPost(col(f2.service)) && col(f1.date) > col(f2.date)
  }

  /**
   * Seleziona tra tutte le misure di cambio misuratore IM1POST/IGMGPOST la più recente, per ogni pdr
   *
   * @param measures [[DataFrame]] le misure tra cui cercare l'IM1POST/IGMGPOST più recenti
   * @return [[DataFrame]] contenente per ogni pdr il flusso IM1POST o IGMGPOST più recente
   */
  private def selectMaxPostMeasures(measures: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy(col(f2.pdr))

    measures.where(isPost(col(f2.service))).withColumn(f2max, max(f2.date).over(windowSpec))
      .where(col(f2.date) === col(f2max)) //.drop(col(colName))
  }

  /**
   * Seleziona per ogni pdr e per ogni data il flusso di misura e il coefficiente di correzione corretto.
   *
   * Dal risultato della Join ad ogni misura sono associati n coefficienti di correzione di tutti gli n flussi IM1/IGMG PRE successivi
   * viene selezionato il coefficiente di correzione del flusso IM1/IGMG PRE più vicino dal punto di vista temporale.
   *
   * @param joinedMeasures [[DataFrame]] risultato della join di matching misure coefficienti
   * @return [[DataFrame]] contenente per ogni pdr e data solo la misura e il coefficiente di correzione corretto
   */
  private def filterJoinedMeasures(joinedMeasures: DataFrame): DataFrame = {

    val priorityColName: String = "priority"

    val windowSpec = Window.partitionBy(col(f1.pdr), col(f1.date), col(priorityColName)).orderBy(asc(f2.date))

    joinedMeasures.withColumn(priorityColName, when(isPre(col(f1.service)), lit("0")).otherwise(lit("1")))
      .withColumn("row_number", row_number.over(windowSpec))
      .where(col("row_number") === 1).select(col(f1.pdr), col(f1.date), col(f1.service), col(f2.coeff_corr))
  }

  /**
   * A partire dal dataframe contenente tutte le misure di tutti i pdr che hanno almeno un flusso IM1/IGMG nel periodo
   * considerato, calcola il dataframe delle misure con associata la colonna coefficiente di correzione.
   *
   * @param dfMeasures [[DataFrame]] di tutte le misure con almeno un IM1/IGMG
   * @return [[DataFrame]] con le misure arricchite del coefficiente di correzione
   */
  private def setCoffOfMeasuresWithChangeMis(dfMeasures: DataFrame): DataFrame = {

    val cmMeasures: DataFrame = dfMeasures.where(isMeterChangeMeasure(col(f1.service)))
      .select(col(f1.pdr).alias(f2.pdr), col(f1.date).alias(f2.date), col(f1.service).alias(f2.service), col(f1.coeff_corr).alias(f2.coeff_corr))

    // while all the pre_coefficient are necessary, only the last post_coefficient is necessary for each pdr
    val im1IgimgPreMeasures = cmMeasures.where(isPre(col(f2.service))).withColumn(f2max, lit(null))
    val im1IgimgMeasures = im1IgimgPreMeasures.union(selectMaxPostMeasures(cmMeasures)).coalesce(im1IgimgPreMeasures.rdd.getNumPartitions)

    // creo il dataframe iniziale di misure associate ai coefficienti secondo tre regole di Join:
    //    associateSameIm1IgmgMeasure   =>   per associare correttamente i coefficienti finali degli Im1/Igmg
    //    associateMeasuresWithPre      =>   per associare ad ogni altra misura tutti i coefficienti pre degli
    //                                       Im1/Igmg successivi
    //    associateMeasuresWithPost     =>   per associare ad ogni altra misura il coefficienti Post dell'ultimo
    //                                       Im1/Igmg precedente
    val joinedMeasures = dfMeasures.join(im1IgimgMeasures,
      isSamePdr &&
        (associateSameIm1IgmgMeasure || associateMeasuresWithPre || associateMeasuresWithPost))

    //filtro le misure in modo tale da associare ad ogni misura un unico coefficiente di correzione
    //elimino i matching "sbagliati" che si sono generati nella join
    filterJoinedMeasures(joinedMeasures)

  }


  /**
   * Calcola la maschera ((cod_pdr,data,cod_servizio), coeff_corr) da applicare ad ogni misura dell'RDD in ingresso per
   * determinare il coefficiente di correzione
   *
   * @param measures [[RDD[Flow]]] misure per le quali dover calcolare il coefficiente di correzione
   * @return Paired [[RDD]] con chiave di tipo [[String,Date,String]] che rappresenta (cod_pdr,data,cod_servizio) della misura,
   *         valore di tipo [[Double]] che rappresenta il valore del coefficiente di correzione da applicare alla segnante
   */
  def get(measures: RDD[Flow]): RDD[((String, Option[Date], String), Double)] = {

    val dfMeasures: DataFrame = toDataFrame(measures) //conversione in Dataframe
    //Filtro le misure in modo da considerare solo le misure dei pdr per cui esiste almeno un flusso CM
    //tra le misure nel periodo considerato (per tutti gli altri gruppi di misure coeff dato da CM è NULL)
    val dfMeasuresFiltered = filterPDRsWithCMMeasure(dfMeasures, Environment.getZInfDate, Environment.getZSupDate, f1.pdr, f1.service)

    //setto il coefficiente al dataframe delle misure con almeno un CM nel periodo
    val dfMeasuresWithcoeff = setCoffOfMeasuresWithChangeMis(dfMeasuresFiltered)
    if (Environment.isLocalMode) { //TO DEBUG
      dfMeasuresFiltered.show()
      dfMeasures.show
      println(s"${Environment.getZInfDate}, ${Environment.getZSupDate}")
      dfMeasuresWithcoeff.show
    }
    //ritorno la mappa con chiave (cod_pdr,data,cod_servizio) e valore = coefficiente (di tipo CM)
    dfMeasuresWithcoeff.rdd.map(r => ((r.getAs[String](f1.pdr),
      Option(Constants.getFormatter(format = formatSql).parse(r.getAs[String](f1.date))),
      r.getAs[String](f1.service)), r.getAs[Double](f2.coeff_corr)))
  }

  /**
   * Converte le misure in input di tipo [[RDD[Flow]]] in un [[DataFrame]]
   *
   * @param measures [[RDD[Flow]]] l'input di tutte le misure
   * @return [[DataFrame]] con le colonne necessarie per il calcolo della maschera dei coefficienti
   */
  private def toDataFrame(measures: RDD[Flow]): DataFrame = {
    Environment.getSqlContext.createDataFrame(
      measures.map({ r =>
        CoeffCorrMask(
          r.pdr,
          Constants.getFormatter(format = formatSql).format(r.date.get),
          r.service,
          r.coefCorr
        )
      })
    )
  }
}

object CoeffCorrController {

  case class Fields(pdr: String, date: String, service: String, coeff_corr: String)

  val f1: Fields = Fields("pdr", "date", "service", "coeff_corr")
  val f2: Fields = Fields("g_pdr", "g_date", "g_service", "coeff_corr_final")
  val f2max = "max"
  val limitCoeff = 30.0

  val format = "yyyy/MM/dd"
  val formatSql = "yyyy-MM-dd"

  val schema: StructType = StructType(List(
    StructField(f1.pdr, StringType, nullable = false),
    StructField(f1.date, StringType, nullable = true),
    StructField(f1.service, StringType, nullable = true),
    StructField(f1.coeff_corr, DoubleType, nullable = true)
  ))

  def getCoeff(flow: Flow, rcu: RcuGasMassivoTech): Double = {
    val coeffCorr = {
      if (flow.coef.isDefined)
        flow.coef.get
      else if (rcu != null && rcu.n_coeff_correzione.isDefined)
        rcu.n_coeff_correzione.get
      else 1.0
    }
    if (coeffCorr <= limitCoeff) coeffCorr else 1.0
  }

  def getCoeff(flow: Flow, rcuStart: RcuGasMassivoTech, rcuEnd: RcuGasMassivoTech): Double = {
    val coeffCorr = {
      if (flow.coef.isDefined)
        flow.coef.get
      else if (rcuEnd != null && rcuEnd.n_coeff_correzione.isDefined)
        rcuEnd.n_coeff_correzione.get
      else if (rcuStart != null && rcuStart.n_coeff_correzione.isDefined)
        rcuStart.n_coeff_correzione.get
      else 1.0
    }
    if (coeffCorr <= limitCoeff) coeffCorr else 1.0
  }
}

