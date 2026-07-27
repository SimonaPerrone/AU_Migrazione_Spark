package it.eng.au.ccgPubblicazione.controller.runnablefile.traits

import it.eng.au.ccgPubblicazione.dao.request.RequestEsitoDao
import it.eng.au.ccgPubblicazione.model.request.RequestEsito
import it.eng.au.ccgPubblicazione.model.requestinfo.{ReportInfo, RequestInfo}
import it.eng.au.ccgPubblicazione.schema.request.RequestPdrSchema
import it.eng.au.ccgPubblicazione.utility.Constants._
import it.eng.au.ccgPubblicazione.utility.FileUtility.{convertStringTimestampToLocalDateTime, isBroadcast, putIntoZip}
import it.eng.au.ccgPubblicazione.utility.{Environment, FileUtility}
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, Row}

import java.io.{File, FileOutputStream}
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.zip.ZipOutputStream
import scala.collection.immutable.ListMap
import scala.collection.mutable

trait RunnableAggregator extends Serializable {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  //  val sessionName: String // AGG,SBG,CDP
  val publicationType: String // DETTAGLIOUNICO,INCOERENTI,INCOERENTIDETTAGLIO,FIN,RIC
  val operationName: String // AGG,SBG,CDP
  val baseNumber: String // 1,2,3
  val ccg: String = CCG

  ///mnt/settlementSYN_parallelo/AGG/AGG1/AGG1_00165480302/2021/6
  ///mnt/isilonshare1_Parallelo/tmp/CCG/AGG/AGG1/DETTAGLIOUNICO/AGG1_00145020244/2022/05/pivautenterichiedente_CCG_AGG_AAAAMM_Timestamp_IDrichiesta_progressivo.csv

  ///mnt/settlementSYN_parallelo/SBG/SBG1/SBG1_00165480302/2021/6
  ///mnt/isilonshare1_Parallelo/tmp/CCG/SBG/SBG1/DETTAGLIOUNICO/SBG1_00338000409/2022/07/pivautenterichiedente_CCG_SBG_AAAAMM_Timestamp_IDrichiesta_progressivo.csv

  ///mnt/settlementSYN_parallelo/CDP/CDP1/CDP1_00165480302/2021/6
  ///mnt/isilonshare1_Parallelo/tmp/CCG/CDP/CDP1/FIN/CDP1_00175180058/2021/12/pivautenterichiedente_CCG_CDP_FIN_AT_Timestamp_Idrichiesta_progressivo.csv
  ///mnt/isilonshare1_Parallelo/tmp/CCG/CDP/CDP1/AGG_RIC/CDP1_00488490012/2022/05/pivautenterichiedente_CCG_CDP_RIC_AT_Timestamp_Idrichiesta_progressivo.csv
  /** Percorso temporaneo di scrittura dei file CSV. */
  def getTmpCsvOutput: String = Environment.getIsilonBasepathTmp + s"/tmp/$ccg/$operationName/$operationName$baseNumber/$publicationType"

  /** Percorso finale di scrittura dei file ZIP. */
  def getPathZipOutput: String = Environment.getIsilonBasepathOut + s"/$operationName/$operationName$baseNumber"

  /** Lunghezza massima dei file CSV in numero di righe. */
  def getCsvMaxRowLength: Some[Long] = Some(Environment.getMaxNumRowFile.toLong)

  /** Data di inzio del processo, utilizzata per generare i timestamp. */
  def getDateToRun: String = Environment.getDateRun

  /** Dimensione massima dei file ZIP. */
  def getMaxDimensionZipFileByte: String = Environment.getMaxDimensionZipFileByte

  def getExecutionId: String = Environment.getConsumptionExecutionid

  /** Mappa tra le colonne del dataframe dei consumi e i campi del CSV da pubblicare. */
  val aggregatoColumnsConsumi: ListMap[String, String]
  /** Mappa tra le colonne del dataframe dei consumi/flussi e i campi del CSV elenco flussi da pubblicare. */
  val aggregatoColumnsFlussi: ListMap[String, String]
  /** Header del file CSV dei consumi. */
  val headerCsvConsumi: List[String]
  /** Header del file CSV dell'elenco flussi. */
  val headerCsvFlussi: List[String]
  /** Mappa tra i campi [[idrichiesta]], [[piva]], [[periodoCompetenza]] e i corrispettivi nel dataframe dei consumi. */
  val keyFieldsConsumi: ListMap[String, String]
  /** Mappa tra i campi [[idrichiesta]], [[piva]], [[periodoCompetenza]] e i corrispettivi nel dataframe dei consumi/flussi. */
  val keyFieldsFlussi: ListMap[String, String]
  /** Utilizzato come progressivo nei file CSV. */
  val counterCsv: String = "counterCsv"
  /** Indica la partita iva principale, ovvero la partita iva dell'utente che ha effettuato una determinata richiesta. */
  val piva = "piva"
  /** Inidica l'id univoco di una richiesta */
  val idrichiesta = "idrichiesta"
  /** Periodo di competenza dei consumi. Può essere annuale (AGG/CDP) o mensile (AGG/SBG). */
  val periodoCompetenza = "periodoCompetenza" //in cdp is only anno
  /** Indica se il periodo di competenza è annuale o mensile. */
  val isAnno: Boolean
  /** Campo del PdR nella tabella dei consumi. */
  val pdrField: String
  /** Campo della data nella tabella dei consumi. */
  val dataField: String

  /**
   * Filtra il dataframe dei consumi [[df]] seguendo le logiche delle rispettive pubblicazioni (Agg, Sbg, Cdp)
   * @param df dataframe dei consumi
   * @return [[df]] filtrato
   */
  def getPdr(df: DataFrame): DataFrame

  /**
   * Ottiene il dataframe dell'elenco flussi da pubblicare.
   * @param df dataframe dei consumi
   * @param validate dataframe dei flussi validati
   * @return dataframe dell'elenco flussi da pubblicare
   */
  def getElencoFlussi(df: DataFrame, validate: DataFrame): DataFrame

  /** Filtro utilizzato in fase di filtraggio della tabella dei consumi. Viene specializzato dai singoli oggetti se necessario. */
  def fileSpecificFilterExpression: Column

  def convertColumnsToString(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((current, c) => current.withColumn(c, col(c).cast(StringType))).na.fill("")
  }

  /**
   * Esegue la procedura di pubblicazione dei file CCG. In particolare,
   *  1. Filtra e prepara alla pubblicazione il dataframe dei consumi ([[getPdR]]);
   *  1. Estrae i PdR senza consumi e li aggiunge al dataframe ottenuto da 1. ([[requestPdrJoinNoConsumption]]);
   *  1. Divide i record per CSV ([[getCsvOutputModel]]) e successivamente li scrive ([[writeCsv]]);
   *  1. Nel caso diverso da CCG3, esegue i punti 1. e 3. per l'elenco flussi ([[getElencoFlussi]], [[getCsvOutputModel]], [[writeCsv]]);
   *  1. Se necessario, effettua l'union dei due dataframe dei PdR e dell'elenco flussi;
   *  1. Comprime i file CSV creati negli step precedenti in uno zip ([[writeZip]]);
   *  1. Nel caso di richieste di tipo PdR, scrive i report di ammissibilità ([[writeAmmissibilita]]);
   *  1. Scrive l'esito della pubblicazione CCG nella relativa tabella ([[writeEsito]]).
   * @param consumption dataframe dei consumi
   * @param validate datafram dei flussi validati
   * @param request dataframe delle richieste
   * @param isPdr indica se le richieste sono di tipo PdR
   */
  def run(consumption: DataFrame, validate: DataFrame, request: DataFrame, isPdr: Boolean): Unit = {
    val pdr = convertColumnsToString(getPdr(consumption))
//    pdr.cache()
//    logger.warn(s"CDP_FIN pdr is ${pdr.count}")
    val pdrWithNoConsumption = if (isPdr) requestPdrJoinNoConsumption(pdr, request) else pdr
//    pdrWithNoConsumption.cache()
//    logger.warn(s"CDP_FIN pdrWithNoConsumption is ${pdrWithNoConsumption.count}")
    val csvOutputModelConsumi = getCsvOutputModel(pdrWithNoConsumption, keyFieldsConsumi)
    val rddCsvPathConsumi = writeCsv(csvOutputModelConsumi, headerCsvConsumi, false)

    val unionRdd = if (s"$ccg$baseNumber" != CCG3) {
      val elencoFlussi = convertColumnsToString(getElencoFlussi(consumption, validate))
      val csvOutputModelFlussi = getCsvOutputModel(elencoFlussi, keyFieldsFlussi)
      val rddCsvPathFlussi = writeCsv(csvOutputModelFlussi, headerCsvFlussi, true)
      rddCsvPathConsumi.union(rddCsvPathFlussi).coalesce(rddCsvPathConsumi.getNumPartitions)
    } else rddCsvPathConsumi

    val unionRddGrouped = unionRdd
      .groupByKey()
      .mapValues(values => values.head)

    val rddInfo = writeZip(unionRddGrouped)

    val prdReportInfo = if (isPdr) {
      pdr.unpersist
      writeAmmissibilita(request)
    } else Environment.sparkContext.emptyRDD[ReportInfo]

    val joinReqInfo = joinRequestRddInfo(request, rddInfo, prdReportInfo, isPdr)
    //    joinReqInfo.cache
    //    logger.warn(s"joinReqInfo: ${joinReqInfo.count}")

    val esito = writeEsito(joinReqInfo, isPdr)
    esito.cache()
    RequestEsitoDao.write(esito)
    RequestEsitoDao.writePartition(esito)
    //decomment for test
//    esito.show(1000, false)
    consumption.unpersist
    esito.unpersist

    //    joinReqInfo.unpersist()
  }

  //this cdp override beacause cdp is only one year
  //this sbg override beacause cdp is only one month
  //  val mesiInAnno: mutable.WrappedArray[String] = mutable.WrappedArray.make(Array("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"))
  val mesiInAnno: String = "01;02;03;04;05;06;07;08;09;10;11;12"

  /**
   * Aggiunge al dataframe [[df]] i PdR delle richieste [[request]] per cui non è stata trovata alcuna corrispondenza nel dataframe dei consumi.
   * Quest'operazione è importante perché per ogni PdR nelle richieste deve essere fornito un esito.
   * @param pdr dataframe dei consumi filtrato
   * @param request richieste di tipo PdR
   * @return dataframe dei consumi filtrato in aggiunta ai PdR per cui non esistono dei consumi
   */
  def requestPdrJoinNoConsumption(pdr: DataFrame, request: DataFrame): DataFrame = {
    val arrayStringsConcat = udf((anno: String, mesi: mutable.WrappedArray[String]) => mesi.map(mese => s"$anno$mese"))
//    val coalesceArray = udf((arraySx: mutable.WrappedArray[String], arrayDx: mutable.WrappedArray[String]) => if (arraySx.isEmpty) arrayDx else arraySx)

    pdr.cache
    val annoMeseRequest = "annoMeseRequest"

    val prepareRequest = request
      //bisogna intercettare soltanto i file ammissibili
      .where(col(RequestPdrSchema.B_AMMISSIBILITA) === AMMISSIBILITA_SI_1)
      .select(
        RequestPdrSchema.N_ID_RICHIESTA,
        RequestPdrSchema.T_ANNO,
        RequestPdrSchema.T_MESE,
        RequestPdrSchema.T_PIVA,
        RequestPdrSchema.T_CODICE_PDR
      ) //.distinct
      //      .withColumn(annoMeseRequest, explode(arrayStringsConcat(col(RequestPdrSchema.T_ANNO), coalesceArray(col(RequestPdrSchema.T_MESE), typedLit(mesiInAnno)))))
      .withColumn(annoMeseRequest, explode(arrayStringsConcat(col(RequestPdrSchema.T_ANNO), split(coalesce(col(RequestPdrSchema.T_MESE), lit(mesiInAnno)), ";"))))

    val requestBroadcast = if (isBroadcast(prepareRequest))
      broadcast(prepareRequest)
    else prepareRequest

    val pdrWithNoConsumption = requestBroadcast
      .join(pdr
        , requestBroadcast(RequestPdrSchema.T_PIVA) === pdr(keyFieldsConsumi(piva)) &&
          requestBroadcast(RequestPdrSchema.T_CODICE_PDR) === pdr(pdrField) &&
          requestBroadcast(annoMeseRequest) === pdr(keyFieldsConsumi(periodoCompetenza))
        , "left")
      .withColumn(keyFieldsConsumi(piva), col(RequestPdrSchema.T_PIVA))
      .withColumn(pdrField, col(RequestPdrSchema.T_CODICE_PDR))
      .withColumn(keyFieldsConsumi(periodoCompetenza), col(annoMeseRequest))
      .withColumn(keyFieldsConsumi(idrichiesta), col(RequestPdrSchema.N_ID_RICHIESTA))

    pdrWithNoConsumption
      .selectExpr(headerCsvConsumi.union(keyFieldsConsumi.values.toSeq).distinct: _*).na.fill("")

  }

  /**
   * Suddivide i record di [[aggFilter]] per CSV tramite la funzione [[distribution]], e crea un key-value RDD,
   * dove la chiave è una mappa che identifica un file CSV (idrichiesta, piva, periodCompetenza, progressivo) e i valori sono i record da inserire
   * in quel CSV.
   * @param aggFilter tabella dei consumi filtrata
   * @param keyFields campi chiave
   * @return un key-value RDD in cui ogni chiave corrisponde a un file CSV da scrivere
   */
  def getCsvOutputModel(aggFilter: DataFrame, keyFields: ListMap[String, String]): RDD[(Map[String, String], Row)] = {
    var dfDistribution = distribution(aggFilter, keyFields.values.toList)

    val columns = aggFilter.columns :+ counterCsv

    columns.foreach(column =>
      dfDistribution = dfDistribution.withColumn(column, col(column).cast(StringType))
    )

    dfDistribution
      .rdd
      .map(row => {
        val keyMap = Map(
          idrichiesta -> row.getAs[String](keyFields(idrichiesta)),
          piva -> row.getAs[String](keyFields(piva)),
          periodoCompetenza -> row.getAs[String](keyFields(periodoCompetenza)),
          counterCsv -> row.getAs[String](counterCsv)
        )

        (keyMap, row)
      })

  }

  /**
   * Suddivide i record in [[df]] in un certo numero di CSV, in modo tale che ogni CSV abbia al più [[getCsvMaxRowLength]] righe.
   * @param df tabella dei consumi
   * @param keys lista delle chiavi su cui effettuare la window
   * @return [[df]] con un campo aggiuntivo, [[counterCsv]], che corrisponde al progressivo del CSV nel quale verrà inserito il record
   */
  def distribution(df: DataFrame, keys: List[String]): DataFrame = {
    val csvMaxNumberRow = getCsvMaxRowLength.get

    val window = Window.partitionBy(keys.map(col): _*)

    df
      .withColumn(counterCsv, count("*").over(window))
      .withColumn(counterCsv, ((monotonically_increasing_id % ceil(col(counterCsv) / csvMaxNumberRow)) + 1).cast(StringType))
      .select(
        df("*"),
        col(counterCsv)
      )
      .repartition((keys :+ counterCsv).map(col): _*)
  }

  /**
   * Esegue la scrittura dei file CSV in una cartella temporanea, splittandoli per [[periodoCompetenza]] e utilizzando [[counterCsv]] come progressivo.
   * @param rddCsvOutput `RDD` in cui la chiave è una mappa (contiene l'id richiesta, le piva, il periodo competenza, il counterCsv), e i valori sono i record da pubblicare per quella chiave.
   * @param columnsField campi da pubblicare
   * @param isElencoFlussi indica se si tratta di scrittura del file elenco flussi
   * @return un `RDD` che associa la mappa (id richiesta, piva, anno competenza) al percorso della cartella in cui sono stati scritti i file CSV
   */
  def writeCsv(rddCsvOutput: RDD[(Map[String, String], Row)], columnsField: List[String], isElencoFlussi: Boolean): RDD[(Map[String, String], String)] = {

    val tmpCsvOutput = getTmpCsvOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)

    //        FileUtility.setYarn777toTmpFolder(tmpCsvOutput)
    FileUtils.deleteDirectory(new File(tmpCsvOutput)) //in windows test comment this
    rddCsvOutput.groupByKey().map({ case (mapKeys, rows) =>

      val countCsv = mapKeys(counterCsv)
      val am = mapKeys(periodoCompetenza)
      val path = tmpCsvOutput + getCsvOutputPath(mapKeys, daterun, countCsv, am, isElencoFlussi)

      val records = rows.toList.map(row => {
        val isNoConsumiPdr = !isElencoFlussi && row.getAs[String](dataField).isEmpty
        columnsField.map(column => {
          if (isNoConsumiPdr && column == keyFieldsConsumi(piva)) "" else row.getAs[String](column) //we need to get an empty piva when writing csv if the record is noConsumi in the PdR case
        }).mkString(CSV_SEPARATOR)
      })

      val header = Some(columnsField.mkString(CSV_SEPARATOR))

      FileUtility.writeCsv(path, header, records, appendMode = true)

      val convertAnnomese = if (isAnno) mapKeys.updated(periodoCompetenza, mapKeys(periodoCompetenza).substring(0, 4)) else mapKeys
      (convertAnnomese.-(counterCsv), path)
    }).groupByKey()
      .mapValues(_.head)
  }

  def getOutputPath(pivaUtente: String, date: LocalDateTime): String = {
    val year = date.getYear
    val month = ("0" + date.getMonthValue.toString).takeRight(2)

    s"$operationName${baseNumber}_$pivaUtente/$year/$month"
  }

  def getCsvOutputPath(
                        mapKey: Map[String, String]
                        , date: LocalDateTime
                        , counterCsv: String
                        , annoMese: String
                        , isElencoFlussi: Boolean
                      ): String = {
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaUtente = mapKey(piva)
    val idRichiesta = mapKey(idrichiesta)
    val elencoFlussiPath = if (isElencoFlussi) "_Elenco_Flussi" else ""

    val outputPath = getOutputPath(pivaUtente, date)

    //es AGG1_123/2020/12/123_INCOERENTI_2020_154898751234_1.csv
    //es AGG1_123/2020/12/123_456_INCOERENTI_2020_154898751234_1.csv
    s"/${outputPath}/$idRichiesta/${pivaUtente}_${ccg}_${operationName}${elencoFlussiPath}_${annoMese}_${timestamp}_${idRichiesta}_${counterCsv}.csv"
  }

  /**
   * Comprime i file CSV in file ZIP, scrivendoli nella cartella finale. Il processo aggiunge i CSV in uno ZIP finché quest'ultimo non supera la soglia determinata da [[getMaxDimensionZipFileByte]];
   * dopodiché chiude lo ZIP ed esegue lo stesso processo con un nuovo ZIP.
   * @param rddCsvPath `RDD` contenente la mappa (id richiesta, piva, periodo competenza) di cui si devono creare gli ZIP e il percorso dove sono stati scritti i relativi file CSV
   * @return `RDD` con le informazioni necessarie per popolare la tabella di reportistica
   */
  def writeZip(rddCsvPath: RDD[(Map[String, String], String)]): RDD[(String, String, Int)] = {
    val tmpCsvOutput = getTmpCsvOutput
    val pathZipOutput = getPathZipOutput
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val maxDimensionZip = getMaxDimensionZipFileByte.toLong

    rddCsvPath.map({ case (mapKeys, path) =>
      val pathInputFileCsv = new File(path)
      val outputFolder = pathInputFileCsv.getParentFile.getParent.replaceAll(tmpCsvOutput, pathZipOutput) //in local windows test this doesn't work
      var zipName = getZipOutputName(mapKeys, daterun)
      val originalZipName = zipName
      val outputFolderIfExists = new File(outputFolder)

      var count = 1
      val pathZip = if (outputFolderIfExists.exists()) {

        var zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
        pathInputFileCsv.getParentFile.listFiles()
          .filter(value => value.getName.substring(value.getName.length - 4).equals(".csv"))
          //          .filter(value => {
          //            val name = value.getName
          //            val matchesPdr = filenameRegexPdrCsv.findFirstMatchIn(name)
          //            val matchesElencoFlussi = filenameRegexElencoFlussiCsv.findFirstMatchIn(name)
          //
          //            (matchesPdr.isDefined && conditionForExtractCsvPdr(matchesPdr.get, mapKeys)) ||
          //              (matchesElencoFlussi.isDefined && conditionForExtractCsvElencoFlussi(matchesElencoFlussi.get, mapKeys))
          //          })
          .foreach { csvFile =>
            val readZip = new File(outputFolder + zipName)
            val dimensionZipFile = readZip.length()
            if (dimensionZipFile < maxDimensionZip) {
              putIntoZip(zip, csvFile)
            }
            else {
              zip.close()
              count += 1
              zipName = originalZipName.replace("_1.zip", "_" + count + ".zip")
              zip = new ZipOutputStream(new FileOutputStream(FileUtility.create777File(outputFolder + zipName)))
              putIntoZip(zip, csvFile)
            }
          }
        zip.close()
        outputFolder + originalZipName
      } else {
        logger.warn(s"Couldn't write to $outputFolder, the path does not exits.")
        ERRORE_DI_SCRITTURA
      }

      (mapKeys(idrichiesta), pathZip, count)
    })
  }

  def getZipOutputName(mapKeys: Map[String, String], today: LocalDateTime): String = {
    //    val year = today.getYear
    val timestamp = today.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val pivaFolder = mapKeys(piva)
    val year = mapKeys(periodoCompetenza)
    val idRequest = mapKeys(idrichiesta)

    val zipName = s"/${pivaFolder}_${ccg}_${operationName}_${year}_${timestamp}_${idRequest}_1.zip"
    zipName
  }

  /**
   * Scrive i file CSV di ammissibilità relativi alle richieste PdR elaborate. Essi sono di due tipi:
   *  - ReportEsitoPdRContatoreConsumi, per il tipo ammissibilità "PDR";
   *  - ReportEsitoFileContatoreConsumi, per il tipo ammissibilità "FILE".
   *  Per maggiori informazioni, consultare i documenti tecnici.
   * @param request richieste di tipo PdR
   * @return un `RDD` contenente le informazioni da inserire nella tabella di reportistica.
   */
  def writeAmmissibilita(request: DataFrame): RDD[ReportInfo] = {
    val headerPdr = ListMap(
      RequestPdrSchema.T_CODICE_PDR.toString -> "PDR",
      RequestPdrSchema.B_AMMISSIBILITA.toString -> "Ammissibilita",
      RequestPdrSchema.T_COD_CAUSALE.toString -> "Codice_Inammissibilita",
      RequestPdrSchema.T_MOTIVAZIONE.toString -> "Descrizione"
    )

    val requestPdr = request
      .filter(col(RequestPdrSchema.T_TIPO_AMM) === AMMISSIBILITA_PDR)
      .select(
        RequestPdrSchema.N_ID_RICHIESTA,
        RequestPdrSchema.T_PIVA,
        RequestPdrSchema.T_CODICE_PDR,
        RequestPdrSchema.B_AMMISSIBILITA,
        RequestPdrSchema.T_COD_CAUSALE,
        RequestPdrSchema.T_MOTIVAZIONE
      )
      .distinct

    //    requestPdr.cache
    //    logger.warn(s"request pdr: ${requestPdr.count}")
    //    requestPdr.cache
    //    logger.warn(s"${publicationType} ${operationName} ${sessionName}${baseNumber} count request with pdr for write ammissibilita ${requestPdr.count}")

    //    val pdr = consumption
    //      .selectExpr(keyFieldsConsumi(idrichiesta), pdrField)
    //      .distinct

    //broadcast(requestPdr)TODO da valutare
    val joinPdr = requestPdr
      //      .join(pdr, requestPdr(RequestPdrSchema.N_ID_RICHIESTA) === pdr(keyFieldsConsumi(idrichiesta)) && requestPdr(RequestPdrSchema.T_CODICE_PDR) === pdr(pdrField), "left")
      //      .withColumn(RequestPdrSchema.T_COD_CAUSALE_PDR, when(requestPdr(RequestPdrSchema.B_AMMISSIBILITA_PDR) === SI_1 && pdr(pdrField).isNull, lit(COD_CAUSALE_PDR)).otherwise(requestPdr(RequestPdrSchema.T_COD_CAUSALE_PDR)))
      //      .withColumn(RequestPdrSchema.T_MOTIVAZIONE_PDR, when(requestPdr(RequestPdrSchema.B_AMMISSIBILITA_PDR) === SI_1 && pdr(pdrField).isNull, lit(MOTIVAZIONE_PDR)).otherwise(requestPdr(RequestPdrSchema.T_MOTIVAZIONE_PDR)))
      .select(
        RequestPdrSchema.N_ID_RICHIESTA,
        RequestPdrSchema.T_PIVA,
        RequestPdrSchema.T_CODICE_PDR,
        RequestPdrSchema.B_AMMISSIBILITA,
        RequestPdrSchema.T_COD_CAUSALE,
        RequestPdrSchema.T_MOTIVAZIONE
      )

    //    joinPdr.cache
    //    logger.warn(s"joinPdr: ${joinPdr.count}")
    val pdrRequestInfo = writeGenericRequestCsv(joinPdr, ReportEsitoPdRContatoreConsumi, headerPdr)

    //write ammissibilità file
    val headerFile = ListMap(
      RequestPdrSchema.T_NOME_FILE.toString -> "File",
      RequestPdrSchema.B_AMMISSIBILITA.toString -> "Ammissibilita",
      RequestPdrSchema.T_COD_CAUSALE.toString -> "Codice_Inammissibilita",
      RequestPdrSchema.T_MOTIVAZIONE.toString -> "Descrizione"
    )

    val requestFile = request
      .filter(col(RequestPdrSchema.T_TIPO_AMM) === AMMISSIBILITA_FILE)
      .select(
        RequestPdrSchema.N_ID_RICHIESTA,
        RequestPdrSchema.T_PIVA,
        RequestPdrSchema.T_NOME_FILE,
        RequestPdrSchema.B_AMMISSIBILITA,
        RequestPdrSchema.T_COD_CAUSALE,
        RequestPdrSchema.T_MOTIVAZIONE
      )
      .distinct

    //    requestFile.cache
    //    logger.warn(s"request file: ${requestFile.count}")
    val fileRequestInfo = writeGenericRequestCsv(requestFile, ReportEsitoFileContatoreConsumi, headerFile)

    pdrRequestInfo.union(fileRequestInfo).coalesce(pdrRequestInfo.getNumPartitions)
  }

  /**
   * Scrive i file CSV di ammissibilità relativo alle richieste [[request]] di tipo [[tipo]].
   * @param request richieste di tipo PdR
   * @param tipo tipologia dell'ammissibilità ("PDR" o "FILE")
   * @param columns mapping tra le colonne del dataframe [[request]] e i campi del file CSV
   * @return un `RDD` contenente le informazioni da inserire nella tabella di reportistica
   */
  def writeGenericRequestCsv(request: DataFrame, tipo: String, columns: ListMap[String, String]): RDD[ReportInfo] = {
    var requestRenamed = request
    columns.foreach({ case (dailyName, fileName) =>
      requestRenamed = requestRenamed.withColumn(fileName, col(dailyName).cast(StringType))
    })

    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val pathZipOutput = getPathZipOutput
    val columnsField = columns.values

    convertColumnsToString(requestRenamed)
      .rdd
      .keyBy(row => Map(
        RequestPdrSchema.N_ID_RICHIESTA.toString -> row.getAs[String](RequestPdrSchema.N_ID_RICHIESTA)
        , RequestPdrSchema.T_PIVA.toString -> row.getAs[String](RequestPdrSchema.T_PIVA)
      ))
      .groupByKey
      .map({ case (key, rows) =>
        val idRichiesta = key(RequestPdrSchema.N_ID_RICHIESTA.toString)
        val piva = key(RequestPdrSchema.T_PIVA.toString)
        val outputFolder = s"$pathZipOutput/${getOutputPath(piva, daterun)}"
        val fileName = getFileAmmisibilitaName(idRichiesta, tipo, daterun)
        val fullPath = s"$outputFolder/$fileName"

        val records = rows.toList.map(row => {
          columnsField.map(column => {
            row.getAs[String](column)
          }).mkString(CSV_SEPARATOR)
        })

        val header = Some(columnsField.mkString(CSV_SEPARATOR))

        val outputFolderExists = new File(outputFolder).exists()
        if (outputFolderExists) {
          FileUtility.writeCsv(fullPath, header, records, appendMode = true)
          ReportInfo(idRichiesta, outputFolder, fileName, writeError = false)
        }
        else ReportInfo(idRichiesta, outputFolder, "", writeError = true)
      })
  }

  def getFileAmmisibilitaName(idrichiesta: String, tipo: String, date: LocalDateTime): String = {
    val timestamp = date.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    s"${tipo}_${idrichiesta}_${timestamp}.csv"
  }

  /**
   * Effettua la join tra le richieste [[requests]], l'RDD contenente le informazioni sugli ZIP creati,
   * e l'RDD contenente le (eventuali) informazioni sui file di ammissibilità creati (quest'ultimo è valorizzato solo nel caso di richieste PdR)
   * @param request dateframe delle richieste
   * @param rddInfo RDD contenente le info sui file ZIP creati
   * @param pdrReportInfo RDD contenente le info sui file CSV di ammissibilità creati (solo nel caso di richieste PdR)
   * @param isPdr boolean che indica se si tratta di richieste di tipo PdR
   * @return RDD per la successiva scrittura su tabella di reportistica
   */
  def joinRequestRddInfo(request: DataFrame, rddInfo: RDD[(String, String, Int)], pdrReportInfo: RDD[ReportInfo], isPdr: Boolean): RDD[RequestInfo] = {
    val rddRequestAmmissibilita = if (isPdr) request
      .groupBy(RequestPdrSchema.N_ID_RICHIESTA, RequestPdrSchema.T_PIVA)
      .agg(
        min(RequestPdrSchema.T_TIPO_AMM).as(RequestPdrSchema.T_TIPO_AMM)
      )
    else request

    val rddRequest = rddRequestAmmissibilita
      .rdd
      .map(row => (
        row.getAs[String](RequestPdrSchema.N_ID_RICHIESTA),
        (
          row.getAs[String](RequestPdrSchema.T_PIVA)
          , if (isPdr) Option(row.getAs[String](RequestPdrSchema.T_TIPO_AMM)) else None
        )
      ))
      .distinct

    val rddInfoPrep = rddInfo.map({ case (idrichiesta, pathZip, count) => (idrichiesta, (pathZip, count)) })

    val pdrReportInfoPrep = pdrReportInfo.map(reportInfo => (reportInfo.idRichiesta, (reportInfo.pathAmmissibilita, reportInfo.fileAmmissibilita, reportInfo.writeError)))

    rddRequest
      .leftOuterJoin(pdrReportInfoPrep)
      .leftOuterJoin(rddInfoPrep)
      .map({ case (idrichiesta, (((piva, tipoAmmissibilita), reportInfo), infoOption)) =>
        RequestInfo(
          idrichiesta = idrichiesta,
          piva = piva,
          pdrReport = reportInfo,
          tipoAmmissibilita = tipoAmmissibilita,
          infoOption = infoOption
        )
      })
  }

  /**
   * Creazione del dataframe di esito della pubblicazione CCG, in vista della successiva scrittura nella relativa tabella.
   * @param joinReqInfo RDD contenente le varie info (file ZIP creati, file CSV di ammissibilità creati, ...) della pubblicazione CCG
   * @param isPdr booleano che indica se siamo nel caso di richieste di tipo PdR
   * @return dataframe esito della pubblicazione CCG
   */
  def writeEsito(joinReqInfo: RDD[RequestInfo], isPdr: Boolean): DataFrame = {
    val daterun = convertStringTimestampToLocalDateTime(getDateToRun)
    val execid = Environment.getConsumptionExecutionid
    val timestampToRun = Timestamp.valueOf(daterun)
    val dataRichiesta = Environment.getDataRichiesta
    val sessione = Environment.getSessione

    Environment.spark.sqlContext.createDataFrame(
      joinReqInfo.map(requestInfo => {
        // Estraimo le informazioni riguardo la scrittura degli ZIP:
        // - pathZip: percorso degli zip (compreso il nome)
        // - count: numero di zip scritti in quella cartella
        // - writeErrorZip: se vi è stato o meno un errore di scrittura in quella cartella
        val (pathZip, count, writeErrorZip) = requestInfo.infoOption.getOrElse(("", 0)) match {
          case (ERRORE_DI_SCRITTURA, count) => ("", count, true)    //nel caso di errore di scrittura, lo stato deve essere IE
          case (pathZip, count) => (pathZip, count, false)
        }

        // Estraiamo le informazioni riguardo la scrittura dei file CSV di ammissibilità:
        // - pathAmmissibilità: percorso dei file di ammissibilità
        // - fileAmmissibilità: nome del file ammissibilità scritto
        // - writeErrorReport: se vi è stato o meno un errore di scrittura in quella cartella
        val (pathAmmissibilita, fileAmmisibilita, writeErrorReport) = requestInfo.pdrReport.getOrElse(("", "", false))
        /** Indica se vi è stato un errore nella scrittura dello ZIP o del CSV di ammissibilità. */
        val internalError = writeErrorZip || writeErrorReport

        val pathInputFileCsv = new File(pathZip)
        val zipFileName =
          if (count > 1)
            (1 to count).map(n => pathInputFileCsv.getName.replace("_1.zip", s"_$n.zip")).mkString(";")
          else
            pathInputFileCsv.getName

        /** Indica lo stato finale della richiesta:
         *  - [[ERRORE_INTERNO]] se vi è stato un errore nella scrittura di uno ZIP o di un CSV di ammissibilità;
         *  - [[ELABORATO]] se non si sono presentati problemi;
         *  - [[NO_CONSUMI]] se nel caso di richiesta di tipo PdR non sono stati trovati consumi;
         *  - [[NON_AMMISSIBILE]] nel caso di [[AMMISSIBILITA_FILE]] (perché se abbiamo una richiesta con t_tipo_amm=FILE, è sicuramente non ammissibile) */
        val stato =
          if (internalError) ERRORE_INTERNO
          else if (requestInfo.tipoAmmissibilita.getOrElse(AMMISSIBILITA_PDR) == AMMISSIBILITA_PDR) {
            if (requestInfo.infoOption.isDefined) ELABORATO
            else NO_CONSUMI
          }
          else NON_AMMISSIBILE

        val nameReportEsito = if (isPdr) Option(fileAmmisibilita) else None
        val tPath = if (isPdr) pathAmmissibilita else pathInputFileCsv.getParent

        RequestEsito(
          N_ID_RICHIESTA = requestInfo.idrichiesta.toLong,
          T_PATH = tPath,
          T_FILE_AMMISSIBILITA = nameReportEsito, // o file o pdr
          T_FILE_ESITO = zipFileName, //tutti i file prodotti separati con un ;
          T_STATO = stato, // E / NA / NC / IE
          T_OPERATION_NAME = operationName + baseNumber,
          T_NUMBER_FILE_ZIP = count,
          EXECUTION_ID_INPUT_READ = execid,
          D_DATA_ESITO = timestampToRun,
          executionid = timestampToRun.getTime,
          TIPO_RICHIESTA = if (isPdr) PDR else FILTRO,
          D_DATA_RICHIESTA = dataRichiesta,
          sessione = sessione
        )
      })
    )
  }
}
