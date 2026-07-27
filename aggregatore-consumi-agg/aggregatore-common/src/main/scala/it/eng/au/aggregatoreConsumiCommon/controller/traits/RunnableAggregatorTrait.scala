package it.eng.au.aggregatoreConsumiCommon.controller.traits

import it.eng.au.aggregatoreConsumiCommon.dao.ClassiGruppiDiMisuraPortataRcugas
import it.eng.au.aggregatoreConsumiCommon.schema.{DailyConsumptionAggSchema, DailyConsumptionInputProcessSchema}
import it.eng.au.aggregatoreConsumiCommon.utility.Environment
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.pdr.PdrDettaglioUnico
import it.eng.au.aggregatoreConsumiCommon.controller.impl.dettaglioUnico.elencoFlussi.ElencoFlussiDettaglioUnico

/** Contiene i metodi e i valori comuni a tutti gli aggregatori. È esteso da
 *  - [[RunnableAggregatorPerfomance]], a sua volta esteso da tutti i processi tranne dettaglio unico
 *  - [[RunnableAggregatorPerfomanceOld]], esteso dai trait [[PdrDettaglioUnico]] e [[ElencoFlussiDettaglioUnico]], che sono utilizzati in
 *  - [[DettaglioUnicoTrait]], che gestisce la pubblicazione del dettaglio unico
 *  */
trait RunnableAggregatorTrait extends Serializable {
  /** Nome della pubblicazione, e.g. AGGREGATO, INCOERENTI */
  val operationName: String
  /** Numero associato al tipo di pubblicazione, e.g. 1 in AGG1 */
  val baseNumber: String

  def run(df: DataFrame): Unit

  /** Percorso base temporaneo in cui vengono scritti i file CSV. e.g. /mnt/isilonshare1_Parallelo/tmp/AGG/AGG1/AGGREGATO */
  def getTmpCsvOutput: String = Environment.getIsilonBasepathTmp + s"/tmp/$getPublicationType/" + getPublicationType + baseNumber + "/" + operationName
  @deprecated
  def getTmpHdfsOutput: String = Environment.getHDFSTmpBasepath + "/" + getPublicationType + baseNumber + "/" + operationName
  /** Percorso base in cui vengono scritti i file ZIP, e.g. /mnt/isilonshare1_Parallelo/AGG/AGG1 */
  def getPathZipOutput: String = Environment.getIsilonBasepathOut + s"/$getPublicationType/" + getPublicationType + baseNumber

  /** Numero massimo di righe per un singolo file CSV */
  def getCsvMaxRowLength: Some[Long] = Some(Environment.getMaxNumRowFile.toLong)
  /** Data di lancio del processo */
  def getDateToRun: String = Environment.getDateRun
  /** Nome della sessione di lancio, e.g. AGG_S1_PRE */
  def getSessionName: String = Environment.getZipSessionName
  /** Nome della pubblicazione, e.g. AGG o SBG */
  def getPublicationType: String = Environment.getPublicationType
  /** Parametro yyyy (AGG) o yyyyMM (SBG) da inserire nel nome dello zip */
  def getYear: String = Environment.getYear
  @deprecated
  def getMaxDimensionZipFile: Long = Environment.getMaxDimensionZipFileByte.toLong
  /** Dimensione soglia per i file ZIP */
  def getMaxSizeThresholdZip: String = Environment.getMaxSizeThresholdZip
  /** Percorso su HDFS per la scrittura della tabella di reportistica */
  def getHdfsOutputBasepathInfoLog: String = Environment.getHDFSOutputBasepath
  def getExecutionId: String = Environment.getDailyConsumptionExecutionid

  // Properties utilizzate negli incoerenti SBG
  def isPdrListaCEnabled: Boolean = Environment.isPdrListaCEnabled.equals("true")
  def isPdrListaDEnabled: Boolean = Environment.isPdrListaDEnabled.equals("true")
  def getPdrListaCCsvPath: String = Environment.getPdrListaCCsvPath
  def getPdrListaDCsvPath: String = Environment.getPdrListaDCsvPath

  def convertColumnsToString(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((current, c) => current.withColumn(c, col(c).cast(StringType)))
  }

  // Colonne utilizzate per l'individuazione degli incoerenti GDM e degli incoerenti EXC
  val dayOfMonth = "dayOfMonth"

  def pdrCountComparisonExpression: Column = when(substring(col(DailyConsumptionAggSchema.annoMese), 5, 2).isin("04", "06", "09", "11"), 30).when(substring(col(DailyConsumptionAggSchema.annoMese), 5, 2).isin("01", "03", "05", "07", "08", "10", "12"), 31).when(substring(col(DailyConsumptionAggSchema.annoMese), 5, 2).isin("02") and pmod(substring(col(DailyConsumptionAggSchema.annoMese), 1, 4).cast(IntegerType), lit(4)) === 0, 29).otherwise(28)

  def windowForComparison: WindowSpec = Window.partitionBy(
    DailyConsumptionAggSchema.pdr,
    DailyConsumptionAggSchema.pivaDistr,
    DailyConsumptionAggSchema.pivaIt,
    DailyConsumptionAggSchema.pivaUdd,
    DailyConsumptionAggSchema.pivaUdb,
    DailyConsumptionAggSchema.pivaRdb,
    DailyConsumptionAggSchema.treatment,
    DailyConsumptionAggSchema.session,
    DailyConsumptionAggSchema.annoMese
  )

  /** Filtro presente nelle funzioni di [[getAggregato]] per permettere a ogni pubblicazione di aggiungere un filtro specifico */
  def fileSpecificFilterExpression: Column = lit(true)
  /** Filtro presente in [[getExcludedPdrs]] per permettere a ogni pubblicazione di aggiungere un filtro specifico */
  def excludedPdrsSpecificCondition: Column = lit(true)
  /** Filtro presente in [[getAnomalousPdrs]] per permettere a ogni pubblicazione di aggiungere un filtro specifico */
  def elencoFlussiSpecificFilterExpression: Column = lit(true)
  /** Filtro presente in [[getAnomalousPdrs]] per permettere ai processi di AGG/SBG di aggiungere un filtro specifico per gli incoerenti GDM.
   * In particolare:
   *  - in AGG non viene applicato nessun filtro;
   *  - in SBG vengono selezionati i pdr con trattamento G o M.
   *  */

  def specificFilterForIncoerentiGdm: Column = lit(true)

  /**
   * Individua i PdR di tipo incoerente GDM, affinché siano pubblicati nella relativa pubblicazione (incoerentiDettaglio).
   * Per maggiori informazioni sui PdR incoerenti GDM, consultare i documenti tecnici oppure la documentazione di [[findAnomalousDays]].
   * @param df dataframe dei consumi
   * @return dataframe contenente soltanto i PdR incoerenti GDM, senza sterilizzazione
   */
  //This method is here since it's shared between ElencoFlussiDettaglioIncoerenti and PdrDettaglioIncoerenti
  def getAnomalousPdrs(df: DataFrame): DataFrame = {
    val aggDf = df
      .na.fill("Y", Seq(DailyConsumptionAggSchema.treatment.toString))
      .filter(
        col(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM) and
        fileSpecificFilterExpression and
        elencoFlussiSpecificFilterExpression and
        specificFilterForIncoerentiGdm
      )

    aggDf
  }

  /**
   * <p>Individua i giorni di incoerenza GDM.</p>
   * Un PdR è incoerente GDM in un certo giorno se il consumo in quel giorno supera la portata massima di quel giorno, definita come
   * <p style="text-align: center;"><i>portata_classe_misuratore * coefficiente_k</i></p>
   * Un PdR risulta incoerente GDM in un mese se il numero di giorni di incoerenza GDM in quel mese è maggiore di 0 e minore di una certa soglia definita da parametro.
   *
   * In questo modo, è possibile individuare i PdR incoerenti GDM e
   *  - pubblicarli nella relativa pubblicazione (incoerentiDettaglio) senza applicare la sterilizzazione,
   *  - oppure pubblicarli nelle altre pubblicazioni sterilizzandone i consumi nei giorni di incoerenza.
   * @param df dataframe dei consumi
   * @return [[df]] con delle colonne aggiuntive per individuare i PdR incoerenti GDM
   */
  // This method is here since it's shared among Aggregator, DettaglioG, Dtg, DettaglioUnico and IncoerentDettaglio
  def findAnomalousDays(df: DataFrame): DataFrame = {
    val thresholdAnomalousDays = Environment.getNumberOfDaysThresholdForGdm.toInt
    val windowByPdrInMonth = Window.partitionBy(col(DailyConsumptionAggSchema.pdr), col(DailyConsumptionAggSchema.annoMese))
    val anomalousDaysCount = "anomalous_days_count"

    val classiGdmToPortataMaxMap = ClassiGruppiDiMisuraPortataRcugas.getAsMap()
    val classiGdmToPortataMax: UserDefinedFunction = udf((classeGdm: String) => classiGdmToPortataMaxMap.get(classeGdm))

    val portataMassima = classiGdmToPortataMax(col(DailyConsumptionAggSchema.classeMisuratore)) * coalesce(col(DailyConsumptionAggSchema.coefficient), lit(1.0))

    df
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, col(DailyConsumptionAggSchema.value) > portataMassima)
      //A PdR is anomalous GDM if and only if the number of anomalous days isn't greater than a given threshold (and greater than 0)
      .withColumn(anomalousDaysCount, sum(col(DailyConsumptionInputProcessSchema.isDayAnomalous).cast(IntegerType)).over(windowByPdrInMonth))
      .withColumn(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM, when(col(anomalousDaysCount) > 0 and col(anomalousDaysCount) < thresholdAnomalousDays, true).otherwise(false))
      //Then, if the PdR is anomalous GDM, we keep the value of isDayAnomalous, otherwise we put it to false
      .withColumn(DailyConsumptionInputProcessSchema.isDayAnomalous, when(col(DailyConsumptionInputProcessSchema.isPdrAnomalousGDM), col(DailyConsumptionInputProcessSchema.isDayAnomalous)).otherwise(false))
      .drop(anomalousDaysCount)
  }

  /**
   * Ottiene la lista dei PdR incoerenti EXC (o esclusi) da includere nelle pubblicazioni di Aggregato, Dettaglio G, Dtg e Dettaglio Unico.
   * Un PdR è considerato escluso in un certo mese se
   *  - è escluso forzatamente (forcedExclusion == true),
   *  - oppure se per tutto il mese soddisfa le condizioni qui presenti (in particolare sull'errorCode)
   * @param df dataframe dei consumi
   * @return dataframe dei consumi contenente soltanto i PdR incoerenti exc
   */
  // This method is here since it's shared among Aggregator, DettaglioG, Dtg and DettaglioUnico
  def getExcludedPdrs(df: DataFrame): DataFrame = {
    val esclusiFlag: String = "esclusiFlag"
    df
      .na.fill("Y", Seq(DailyConsumptionAggSchema.treatment.toString))
      .filter(col(esclusiFlag)
        and col(DailyConsumptionAggSchema.codProfStd).isNotNull
        and fileSpecificFilterExpression
        and excludedPdrsSpecificCondition //specific condition for each class (e.g. DettaglioG wants only pdrs with G treatment)
      )
  }
}
