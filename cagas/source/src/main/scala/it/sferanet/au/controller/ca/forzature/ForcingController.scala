package it.sferanet.au.controller.ca.forzature

import it.sferanet.au.controller.ca.forzature.ForcingController._
import it.sferanet.au.model.MeasureValueType.{C, K}
import it.sferanet.au.model.prestazionale.{IgmgPost, IgmgPre, IgmrPost, IgmrPre, Im1Post, Im1Pre}
import it.sferanet.au.model.{Flow, ForcingMask, MeasureValueType}
import it.sferanet.au.utilities.DataframeUtils._
import it.sferanet.au.utilities.{Constants, DataframeUtils, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, functions}

import java.util.Date

class ForcingController {

  private def isSamePdr: Column = {
    DataframeUtils.isSamePdr(f1.pdr, f2.pdr)
  }

  private def associateMeasuresWithPre: Column = {
    isPre(col(f2.service)) && col(f1.date) < col(f2.date)
  }

  private def associateMeasuresWithPost: Column = {
    isPost(col(f2.service)) && col(f1.date) >= col(f2.date)
  }

  /**
   * Seleziona per ogni pdr e per ogni data il flusso di misura e il la forzatura corrispondente (la più vicina temporalmente).
   *
   * Dal risultato della Join ad ogni misura sono associati n codici di forzatura
   * (PRE di tutti gli n flussi IM1/IGMG PRE successivi)
   * (oppure POST di tutti IM1/IGMG POST precedenti)
   *
   * viene selezionato il coefficiente di correzione del flusso IM1/IGMG più vicino dal punto di vista temporale.
   *
   * @param joinedMeasures [[DataFrame]] risultato della join di matching misure coefficienti
   * @return [[DataFrame]] contenente per ogni pdr e data solo la misura e il coefficiente di correzione corretto
   */
  private def filterJoinedMeasures(joinedMeasures: DataFrame): DataFrame = {

    val dateDiffColName: String = "dateDiff" //colonna per determinare la forzatura temporalmente più vicina

    val measures = joinedMeasures
      .withColumn(dateDiffColName, when(unix_timestamp(col(f1.date).cast(DateType)) > unix_timestamp(col(f2.date).cast(DateType)), unix_timestamp(col(f1.date).cast(DateType)) - unix_timestamp(col(f2.date).cast(DateType)))
        .otherwise(unix_timestamp(col(f2.date).cast(DateType)) - unix_timestamp(col(f1.date).cast(DateType))))

    val windowSpec = Window.partitionBy(col(f1.pdr), col(f1.date), col(f1.service))
      .orderBy(asc(dateDiffColName)) //ordino in base alla differenza tra le date (vicinanza temporale)

    measures
      .withColumn("row_number", row_number.over(windowSpec))
      .where(col("row_number") === 1) //seleziono la più vicina temporalmente
      .withColumn(forcingCodeColName, concat(col(f2.service), col(f2.cau_int_cor))) //codice forzatura dato dalla concatenazione di service + cau_int_cor

  }

  /**
   * true se il cau_int_cor dell'IM1 definisce una forzatura
   *
   * @param cau_int_cor
   * @return true se il cau_int_cor dell'IM1 definisce una forzatura
   */
  def isIm1ForceCode(cau_int_cor: Column): Column = {
    cau_int_cor === 3 || cau_int_cor === 5
  }

  /**
   * true se il cau_int_cor dell'IGMG definisce una forzatura
   *
   * @param cau_int_cor
   * @return true se il cau_int_cor dell'IGMG definisce una forzatura
   */
  def isIgmgForceCode(cau_int_cor: Column): Column = {
    cau_int_cor === 2 || cau_int_cor === 3 || cau_int_cor === 4
  }

  def isIgmrForceCode(cau_int_cor: Column): Column = {
    cau_int_cor === 2 || cau_int_cor === 3 || cau_int_cor === 4
  }


  /**
   * Controlla se il record e' associato ad una forzatura
   *
   * @param service
   * @param cau_int_cor
   * @return True se il record non è associato ad una forzatura (Se la colonna del servizio forzatura o cau_int_cor forzatura sono NULL
   *         oppure service IM1 e cau_int_cor non e' 3 o 5
   *         oppure service IGMG e cau_int_cor non e' 2, 3 o 4)
   */
  def isUnforcedOrUnmerged(service: Column, cau_int_cor: Column): Column = {
    service.isNull || cau_int_cor.isNull ||
      (isIm1(service) && functions.not(isIm1ForceCode(cau_int_cor))) ||
      (isIgmg(service) && functions.not(isIgmgForceCode(cau_int_cor))) ||
      (isIgmr(service) && functions.not(isIgmrForceCode(cau_int_cor)))
  }

  /**
   * Controlla se il record e' associato ad una forzatura
   *
   * @param service
   * @param cau_int_cor
   * @return True se il record non è associato ad una forzatura (Se la colonna del servizio forzatura o cau_int_cor forzatura sono NULL
   *         oppure service IM1 e cau_int_cor non e' 3 o 5
   *         oppure service IGMG e cau_int_cor non e' 2, 3 o 4)
   */
  def isUnforced(service: Column, cau_int_cor: Column): Column = {
    service.isNotNull && cau_int_cor.isNull ||
      (isIm1(service) && functions.not(isIm1ForceCode(cau_int_cor))) ||
      (isIgmg(service) && functions.not(isIgmgForceCode(cau_int_cor))) ||
      (isIgmr(service) && functions.not(isIgmrForceCode(cau_int_cor)))
  }

  //true se service = IM1 e cau_int_cor = 3
  def isIm13(service: Column, cau_int_cor: Column): Column = {
    isIm1(service) && cau_int_cor === 3
  }

  //true se service = IGMG e cau_int_cor = 2
  def isIgmg2(service: Column, cau_int_cor: Column): Column = {
    isIgmg(service) && cau_int_cor === 2
  }

  //true se service = IM1 e cau_int_cor = 3 oppure 5 oppure service = IGMG e cau_int_cor = 2 o 3 o 4
  def isforced(service: String, cau_int_cor: String): Column =
    (isIm1(col(service)) && isIm1ForceCode(col(cau_int_cor))) ||
      (isIgmg(col(service)) && isIgmgForceCode(col(cau_int_cor))) ||
      (isIgmr(col(service)) && isIgmrForceCode(col(cau_int_cor)))

  // ottiene il service POST della misura PRE corrispondente: se IM1PRE -> IM1POST, altrimenti -> IGMGPOST
  def getPostServiceCode(service: Column): Column = {
    when(isIm1(service), Im1Post.serviceName).when(isIgmr(service), IgmrPost.serviceName).otherwise(IgmgPost.serviceName)
  }

  // true se service = IM1 e cau_int_cor = 3 oppure service = IGMG e cau_int_cor = 2
  def isPreDisalligned(service: Column, cau_int_cor: Column): Column = {
    (isIm1(service) && cau_int_cor === 3) ||
      (isIgmg(service) && cau_int_cor === 2) ||
      (isIgmr(service) && cau_int_cor === 2)
  }

  /**
   * (Unused) - in caso di molteplici consecutivi IM1 con cau_int_cor =3 oppure IGMG con cau_int_cor = 2
   * considera solo il più vecchio per applicare le forzatura in avanti (K)
   * in modo piu' prioritario rispetto alla forzatura in indietro (C)
   *
   * @param preCMMeasures [[DataFrame]] delle sole misure di cambio misuratore
   * @return [[DataFrame]] filtrato in modo da eliminare se presenti tra gruppi di  IM1 con cau_int_cor =3
   *         oppure IGMG con cau_int_cor = 2 consecutivi solo i piu' vecchi
   */
  def filterNotConsecutiveIm13Igmg2(preCMMeasures: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy(col(f2.pdr)).orderBy(col(f2.date))

    val deleteColName: String = "delete" //true per i record da eliminare
    //val forcingCodeColName:String = "forcingCode"

    val measures = preCMMeasures
      //.withColumn(forcingCodeColName,concat(col(f2.service),col(f2.cau_int_cor)))
      .withColumn(deleteColName, when(isPreDisalligned(col(f2.service), col(f2.cau_int_cor)) //se IM1(3) o IGMG(2)
        && (isPreDisalligned(lag(f2.service, 1).over(windowSpec), lag(f2.cau_int_cor, 1).over(windowSpec))), //se precedente IM1(3) o IGMG(2)
        lit(true)).otherwise(lit(false)))

    measures.where(col(deleteColName) === false).drop(col(deleteColName))
  }

  /**
   * Filtro Dopo un IGMGPOST(2) elimino tutti IM1POST (cau_int_cor qualsiasi) o IGMGPOST (cau_int_cor diverso da 3)
   * Dopo un IM1POST(3) elimino tutti IGMGPOST(cau_int_cor diverso da 3)
   *
   * in modo tale da prolungare la forzatura POST dell'IM1(3) fino ad un IM1 con cau_int_cor diverso da 3
   * e prolungare la forzatura POST dell'IGMG(2) fino a IGMG(3)
   *
   * @param postCMMeasures [[DataFrame]] delle sole misure di cambio misuratore
   * @return [[DataFrame]] filtrato in modo da eliminare dopo IGMGPOST(2) tutti IM1POST (cau_int_cor qualsiasi)
   *         o IGMGPOST (cau_int_cor diverso da 3)
   *         dopo IM1POST(3) tutti IGMGPOST (cau_int_cor diverso da 3)
   */
  def filterNotForcedIm1IgmgAfterIgmg2(postCMMeasures: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy(col(f2.pdr)).orderBy(col(f2.date))

    val deleteColName: String = "delete"

    //val forcingCodeColName:String = "forcingCode"

    postCMMeasures
      //Filtraggio per eliminare tutti gli Igmg non forzati consecutivi tranne il primo
      //    -> è l'unico che mi serve per bloccare la forzatura post del flusso precedente
      .withColumn(deleteColName, when((isUnforced(col(f2.service), col(f2.cau_int_cor))
        && isUnforced(lag(f2.service, 1).over(windowSpec),
        lag(f2.cau_int_cor, 1).over(windowSpec))), lit(true)).otherwise(lit(false)))
      .where(col(deleteColName) === false || isIm1(col(f2.service))).drop(col(deleteColName))

      //Filtraggio per eliminare igmg non forzato eventualmente presente subito dopo IM1(3)
      //      -> perchè IM1(3) in forzatura post si blocca solo con IGMG(3) o IM1(!3)
      .withColumn(deleteColName, when(isUnforced(col(f2.service), col(f2.cau_int_cor)) && isIgmg(col(f2.service))
        && isIm13(lag(f2.service, 1).over(windowSpec),
        lag(f2.cau_int_cor, 1).over(windowSpec)), true).otherwise(false))
      .where(col(deleteColName) === false).drop(col(deleteColName))

      //Filtraggio per eliminare tutte i flussi tecnici (IM1 E IGMG) non forzati consecutivi tranne il primo
      //    -> è l'unico che mi serve per bloccare la forzatura post del flusso precedente
      .withColumn(deleteColName, when(isUnforced(col(f2.service), col(f2.cau_int_cor))
        && isUnforced(lag(f2.service, 1).over(windowSpec),
        lag(f2.cau_int_cor, 1).over(windowSpec)), true).otherwise(false))
      .where(col(deleteColName) === false).drop(col(deleteColName))

      //Filtraggio per eliminare tutte i flussi tecnici non forzati successivi a un IGMG(2)
      //    -> IGMG(2) in forzatura POST si blocca solo con IGMG(3) o con eventuale sovrapposizione altra Forzatura
      .withColumn(deleteColName, when(isUnforced(col(f2.service), col(f2.cau_int_cor)) &&
        isIgmg2(lag(f2.service, 1).over(windowSpec), lag(f2.cau_int_cor, 1).over(windowSpec)), true)
        .otherwise(false))
      .where(col(deleteColName) === false).drop(col(deleteColName))

  }

  /**
   * Separa il dataframe di flussi di CM di partenza in due dataframe: uno contenente tutti IM1POST/IGMGPOST uno
   * contenente tutti IM1PRE/IGMGPRE
   *
   * @param dfMeasures [[DataFrame]] dei flussi di cambio misuratore
   * @return ([[DataFrame]],[[DataFrame]]) (flussiCMPre, FlussiCMPOST)
   */
  def selectPrePostCMMeasures(dfMeasures: DataFrame): (DataFrame, DataFrame) = {

    val preCMMeasures = dfMeasures.where(isPre(col(f1.service))) //Tutti i CM pre
      .select(col(f1.pdr).alias(f2.pdr), col(f1.date).alias(f2.date), col(f1.service).alias(f2.service),
        col(f1.cau_int_cor).alias(f2.cau_int_cor))

    //val preCMMeasuresFiltered = filterNotConsecutiveIm13Igmg2(preCMMeasures)

    // Dataframe che serve per fare il match con le forzature POST per tutte le misure senza forzatura pre
    val postCMMeasures = preCMMeasures.withColumn(f1.service, getPostServiceCode(col(f2.service)))
      .select(col(f2.pdr), col(f2.date), col(f1.service).alias(f2.service), col(f2.cau_int_cor))

    (preCMMeasures, postCMMeasures)

  }

  /**
   * A partire dal dataframe contenente tutte le misure di tutti i pdr che hanno almeno un flusso IM1/IGMG nel periodo
   * considerato, calcola il dataframe delle misure con associata la colonna codice di forzatura
   *
   * @param dfMeasures [[DataFrame]] di tutte le misure con almeno un IM1/IGMG
   * @return [[DataFrame]] con le misure arricchite del codice di forzatura
   */
  private def setForcingCodeOfMeasuresWithChangeMis(dfMeasures: DataFrame): DataFrame = {

    // seleziono solo IM1PRE o IGMGPRE O IGMRPRE - cioè solo i flussi che mi portano l'informazione cau_int_cor
    val (preCMMeasures, postCMMeasures) = selectPrePostCMMeasures(dfMeasures)

    // creo il dataframe iniziale di misure associate alle forzature PRE per ogni possibile codice di forzatura
    val joinedMeasuresPre = dfMeasures.join(preCMMeasures, isSamePdr && associateMeasuresWithPre, "leftOuter")

    // filtro per eliminare le forzature PRE precedenti ad altre in caso di molteplici matching di forzature
    val filteredJoinedMeasuresPre = filterJoinedMeasures(joinedMeasuresPre)

    // In measureForced_part1 ci sono le misure forzate con forzatura pre => la prima parte del risultato finale
    // => la forzatura pre è prioritaria rispetto all'eventuale forzatura post
    val measuresForced_part1 = filteredJoinedMeasuresPre.where(isforced(f2.service, f2.cau_int_cor))
      .select(col(f1.pdr), col(f1.date), col(f1.service), col(forcingCodeColName))

    // a tutte le misure senza forzatura PRE provo ad associare una forzatura POST
    val joinedMeasuresUnForced = filteredJoinedMeasuresPre.where(isUnforcedOrUnmerged(col(f2.service), col(f2.cau_int_cor)))
      .select(col(f1.pdr), col(f1.date), col(f1.service))

    // creo il dataframe di misure non ancora forzate associate alle forzature POST per ogni possibile codice di forzatura
    val joinedMeasuresPost = joinedMeasuresUnForced.join(postCMMeasures, isSamePdr && associateMeasuresWithPost, "leftOuter")

    // filtro per eliminare le forzature POST successive ad altre in caso di molteplici matching di forzature
    val measuresForced_part2 = filterJoinedMeasures(joinedMeasuresPost)
      .where(isforced(f2.service, f2.cau_int_cor)) //seleziono solo le misure forzate
      .select(col(f1.pdr), col(f1.date), col(f1.service), col(forcingCodeColName))

    measuresForced_part1.union(measuresForced_part2).coalesce(measuresForced_part1.rdd.getNumPartitions)

  }


  /**
   * Calcola la maschera ((cod_pdr,data,cod_servizio), codice forzatura) da applicare ad ogni misura dell'RDD in ingresso per
   * determinare il codice della forzatura da applicare (vedi codici forzatura in ForcingCode object)
   *
   * @param measures [[RDD[Flow]]] misure per le quali dover calcolare il codice forzatura
   * @return Paired [[RDD]] con chiave di tipo [[String,Date,String]] che rappresenta (cod_pdr,data,cod_servizio) della misura,
   *         valore di tipo [[Option[Int]]] che rappresenta il valore del codice forzatura da applicare alla segnante
   */
  def getMap(measures: RDD[Flow]): RDD[((String, Option[Date], String), String)] = {

    val dfMeasures: DataFrame = toDataFrame(measures) //converto in Dataframe

    //prelevo solo le misure dei pdr con almeno un flusso Im1/IGMG/IGMR (per le altre forzatura sarà NULL)
    val dfMeasuresFiltered = filterPDRsWithCMMeasure(dfMeasures, Environment.getZInfDate, Environment.getZSupDate, f1.pdr, f1.service)

    // aggiungo i codici forzatura alle misure forzate (alle misure non forzate forzatura a NULL)
    val dfMeasuresWithforcingCode = setForcingCodeOfMeasuresWithChangeMis(dfMeasuresFiltered)

    //ritorno la maschera RDD con chiave = (cod_pdr,data,cod_servizio), valore = codice forzatura
    dfMeasuresWithforcingCode.rdd.map(r => ((r.getAs[String](f1.pdr),
      Option(Constants.getFormatter(format = formatSql).parse(r.getAs[String](f1.date))),
      r.getAs[String](f1.service)), r.getAs[String](forcingCodeColName)))
  }

  def putForcingCodeToMeasures(measures: RDD[Flow]): RDD[Flow] = {
    //ottengo la maschera RDD con chiave = (cod_pdr,data,cod_servizio), valore = codice forzatura
    val forcingMap = getMap(measures)

    measures
      .map(m => ((m.pdr, m.date, m.service), m)) //metto in leftOuterJoin le misure con la maschera delle misure forzate
      .leftOuterJoin(forcingMap) // v._1 chiave (m.pdr,m.date,m.service)  v._2._1 flusso, v._2._2 forcingCode
      .map(v => v._2._1.changeForcing(v._2._2)) //setto il codice forzatura delle sole misure forzate nell'oggetto Flow
  }

  /**
   * Converte le misure in input di tipo [[RDD[Flow]]] in un [[DataFrame]]
   *
   * @param measures [[RDD[Flow]]] l'input di tutte le misure
   * @return [[DataFrame]] con le colonne necessarie per il calcolo della maschera delle forzature
   */
  private def toDataFrame(measures: RDD[Flow]): DataFrame = {
    Environment.getSqlContext.createDataFrame(
      measures.map({ r =>
        ForcingMask(
          r.pdr,
          Constants.getFormatter(format = formatSql).format(r.date.get),
          r.service,
          Flow.getCauIntCor(r)
        )
      })
    )
  }
}

object ForcingController {

  case class Fields(pdr: String, date: String, service: String, cau_int_cor: String)

  val f1: Fields = Fields("pdr", "date", "service", "cau_int_cor")
  val f2: Fields = Fields("g_pdr", "g_date", "g_service", "g_cau_int_cor")

  val format = "yyyyMMdd"
  val formatSql = "yyyy-MM-dd"

  val forcingCodeColName: String = "forcing_code"

  val schema: StructType = StructType(List(
    StructField(f1.pdr, StringType, nullable = false),
    StructField(f1.date, StringType, nullable = true),
    StructField(f1.service, StringType, nullable = true),
    StructField(f1.cau_int_cor, IntegerType, nullable = true)
  ))

  def getForcingCode(forcingCode: Option[String]): Option[MeasureValueType.Value] = {
    if (forcingCode.isDefined) getForcingCode(forcingCode.get)
    else None
  }

  def getForcingCode(forcingCode: String): Option[MeasureValueType.Value] = {
    if (forcingCode == null || forcingCode.isEmpty) return None

    (forcingCode.substring(0, forcingCode.length - 1), forcingCode.last) match {

      case (Im1Pre.serviceName, '3') => Some(C)
      case (Im1Post.serviceName, '3') => Some(K)

      case (IgmgPre.serviceName, '2') => Some(C)
      case (IgmgPost.serviceName, '2') => Some(K)

      case (IgmrPre.serviceName, '2') => Some(C)
      case (IgmrPost.serviceName, '2') => Some(K)

      case (Im1Pre.serviceName, '5') => Some(K)
      case (Im1Post.serviceName, '5') => Some(C)

      case (IgmgPre.serviceName, '3') => Some(K)
      case (IgmgPost.serviceName, '3') => Some(C)

      case (IgmrPre.serviceName, '3') => Some(K)
      case (IgmrPost.serviceName, '3') => Some(C)

      case (IgmgPre.serviceName, '4') => Some(C)
      case (IgmgPost.serviceName, '4') => Some(C)

      case (IgmrPre.serviceName, '4') => Some(C)
      case (IgmrPost.serviceName, '4') => Some(C)

      case _ => None
    }
  }
}
