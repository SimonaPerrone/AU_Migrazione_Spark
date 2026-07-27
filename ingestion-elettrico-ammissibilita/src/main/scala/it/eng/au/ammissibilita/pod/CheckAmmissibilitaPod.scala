package it.eng.au.ammissibilita.pod

import it.eng.au.ammissibilita.{CheckAmmissibilitaEE, CheckAmmissibilitaRules}
import it.eng.au.args.AmmissibilitaParameters
import it.eng.au.model.{ReportEsitoPODMessage, XMLMetadata}
import it.eng.au.schema.GenericXmlSchema._
import it.eng.au.schema._
import it.eng.au.schema.ammissibilita.AmmissibilitaPodSchema
import it.eng.au.schema.ammissibilita.AmmissibilitaPodSchema._
import it.eng.au.utility._
import it.eng.au.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}
import org.apache.spark.storage.StorageLevel
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.Try
import scala.xml.{Node, XML}

object CheckAmmissibilitaPod extends CheckAmmissibilitaEE {
  def run(args: Array[String], tripleAdmissibleFilesRcuAziendaRcuUdd: (RDD[XMLMetadata], DataFrame, DataFrame)): Unit = {
    val (admissibleFiles, rcuAziendaPDF, rcuUddPDF) = tripleAdmissibleFilesRcuAziendaRcuUdd

    val params = parseAmmissibilitaArgs(args)

    val (inputPath: String, outputPath: String) = getInputOutput(params)

    log.info(s"args: ${args.mkString(";")}")
    log.info(s"Writing into: $outputPath")


    val admissibleFilesWithoutxsd = admissibleFiles.map(meta => meta.copy(flussoSMISXSDBroad = null, flusso1XSDBroad = null, flusso2XSDBroad = null))
    //    admissibleFilesWithoutxsd.persist(StorageLevel.MEMORY_AND_DISK)
    //    log.info(s"Count di admissibleFilesWithoutxsd: " + admissibleFilesWithoutxsd.count)


    val fileWithMessages =
      if (params.isSmis) {
        val rcuPod = ReaderUtility.readAndPrepareRcuPod
        val rcusPod = ReaderUtility.readAndPrepareRcusPod
        val rcuPodDistr = ReaderUtility.readAndPrepareRcuPodDistr
        val rcusPodDistr = ReaderUtility.readAndPrepareRcusPodDistr
        val rcuPodUdd = ReaderUtility.readAndPrepareRcuPodUdd
        val rcusPodUdd = ReaderUtility.readAndPrepareRcusPodUdd
        val rcuPodStato = ReaderUtility.readAndPrepareRcuPodStato(rcuPod)

        val (resultid18, resultid19) = recoverRddXmlWithMetaSMIS(rcuAziendaPDF, rcuPod, rcusPod, rcuPodDistr, rcusPodDistr, rcuPodUdd, rcusPodUdd, rcuUddPDF)

        validateSMIS(admissibleFilesWithoutxsd.repartition(3000), resultid18, resultid19, rcuPodStato)
          .persist(StorageLevel.MEMORY_AND_DISK)
      }
      else validate(admissibleFilesWithoutxsd).persist(StorageLevel.MEMORY_AND_DISK)

    val fileWithErrorMessages = fileWithMessages
      .map({ case (meta, messageList) =>
        val errorMessages = messageList.filter(_.ammissibilita.equalsIgnoreCase(Constants.NO))
        (meta, errorMessages)
      })
      .filter({ case (meta, messageList) => messageList.nonEmpty })


    writeAggregatedPodTxtReport(fileWithErrorMessages, outputPath, params)

    val messagesRDD = fileWithMessages.flatMap({ case (fileXmlWithMeta, messages) =>
      messages.map(message => message.copy(flusso = fileXmlWithMeta.flusso, anno = fileXmlWithMeta.params.year, mese = fileXmlWithMeta.params.month, giorno = fileXmlWithMeta.params.day))
    })

    writeOnHive(messagesRDD)
  }

  /*  def getFilesWithMetadata(params: AmmissibilitaParameters, inputPath: String): RDD[XMLMetadata] = {

      val days: RDD[File] = getRDDDaysFolder(inputPath, params)

      val fileNameRegexToUse = if (params.isSmis) filenameRegexSMIS else filenameRegex //if is a smis flow then it use smis regex else it use original regex

      val filesWithMatches = days.flatMap(_.listFiles()).map(
        file => (file, fileNameRegexToUse.findFirstMatchIn(file.getName))
      ).filter({ case (_, matches) => matches.isDefined })

      filesWithMatches.repartition(filesWithMatches.partitions.length)
        .map({ case (file, matches) =>
          //if is a smis flow then it create smis XMLMetadata else it create original XMLMetadata
          if (params.isSmis) getXMLMetadataSMIS(file, matches, params) else getXMLMetadata(file, matches, params)
        })
    }*/

  def validate(rddXmlMetadata: RDD[XMLMetadata]): RDD[(XMLMetadata, List[ReportEsitoPODMessage])] = {
    val fileWithMessages = rddXmlMetadata.map(fileXmlWithMeta => {
      val tryXml = Try(XML.loadFile(fileXmlWithMeta.file))
      if (tryXml.isSuccess) {
        val xml = tryXml.get
        val codFlusso = (xml \\ FlussoMisure \ CodFlusso).text

        val datiPods = xml \\ FlussoMisure \\ DatiPod

        val messages = datiPods.map(datiPod => {
          //if is a smis flow then it use smis ammissibilita else it use original ammissibilita
          val checker: CheckAmmissibilitaRules[ReportEsitoPODMessage] = CheckAmmissibilitaPodRules
          checker.check(datiPod, fileXmlWithMeta.copy(codFlusso = codFlusso))
        }).toList
        (fileXmlWithMeta, messages)
      }
      else {
        val messages = List(getExceptionMessage(fileXmlWithMeta))
        (fileXmlWithMeta, messages)
      }

    })

    fileWithMessages.map({ case (meta, messageList) =>
      val newMessages = messageList.map(getMessageWithNewLogic(_).asInstanceOf[ReportEsitoPODMessage])
      (meta, newMessages)
    })
  }

  def validateSMIS(rddXmlMetadata: RDD[XMLMetadata], unionPodDistrId18: DataFrame, unionPodDistrId19: DataFrame, rcuPodStato: DataFrame): RDD[(XMLMetadata, List[ReportEsitoPODMessage])] = {

    val resultId18 = unionPodDistrId18.rdd
      .map(row => ((row.getAs[String](RcuPodPSchema.t_codice_pod).take(14), row.getAs[String](RcuAziendaPSchema.t_piva)),
        (DateTimeUtility.getDateTimeOr(row.getAs[String](RcusPodDistrPSchema.d_inizio), "yyyy-MM-dd HH:mm:ss.S", "min").toLocalDate,
          DateTimeUtility.getDateTimeOr(row.getAs[String](RcusPodDistrPSchema.d_fine), "yyyy-MM-dd HH:mm:ss.S", "max").toLocalDate)
      )).groupByKey()

    val resultId19 = unionPodDistrId19.rdd
      .map(row => ( //(
        row.getAs[String](RcuPodPSchema.t_codice_pod).take(14)
        //, row.getAs[String](RcuAziendaPSchema.t_piva))
        ,
        (DateTimeUtility.getDateTimeOr(row.getAs[String](RcusPodUddPSchema.d_inizio), "yyyy-MM-dd HH:mm:ss.S", "min").toLocalDate,
          DateTimeUtility.getDateTimeOr(row.getAs[String](RcusPodUddPSchema.d_fine), "yyyy-MM-dd HH:mm:ss.S", "max").toLocalDate,
          row.getAs[String](RcuUddPSchema.t_codice_terna))
      )).groupByKey()

    val rcuPodStatoForValidation = rcuPodStato.rdd
      .map(row => (
        row.getAs[String](RcuPodPSchema.t_codice_pod).take(14),
        (DateTimeUtility.getDateTimeOr(row.getAs[String](RcuPodStatoPSchema.d_attivazione), "yyyy-MM-dd HH:mm:ss.S", "min").toLocalDate,
          DateTimeUtility.getDateTimeOr(row.getAs[String](RcuPodStatoPSchema.d_disattivazione), "yyyy-MM-dd HH:mm:ss.S", "max").toLocalDate,
          Option(row.getAs[String](RcuPodStatoPSchema.t_stato_attivazione)))
      )).groupByKey()

    val rddXmlMetaWithListNode = rddXmlMetadata.map(fileXmlWithMeta => {
      val tryXml = Try(XML.loadFile(fileXmlWithMeta.file))
      if (tryXml.isSuccess) {
        val xml = tryXml.get
        val datiPods = xml \\ FlussoMisure \\ DatiPod
        val codFlusso = (xml \\ FlussoMisure \ CodFlusso).text

        (fileXmlWithMeta.copy(codFlusso = codFlusso), datiPods.toList)
      }
      else {
        (fileXmlWithMeta, List[Node]())
      }
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val rddXmlMetaWithNodeNonEmpty = rddXmlMetaWithListNode.filter({ case (fileXmlWithMeta, listNode) => listNode.nonEmpty })
    val rddXmlMetaWithNodeIsEmpty = rddXmlMetaWithListNode.filter({ case (fileXmlWithMeta, listNode) => listNode.isEmpty }).keys
      .map(fileXmlWithMeta => {
        val messages = List(getExceptionMessage(fileXmlWithMeta))
        (fileXmlWithMeta, messages)
      })

    var count = 0
    val rddXmlMetaWithNode = rddXmlMetaWithNodeNonEmpty.flatMap({ case (fileXmlWithMeta, listNode) =>
      count += 1
      val fileXmlWithMetaWithID = fileXmlWithMeta.copy(idXml = count)
      listNode.map(node => {
        (fileXmlWithMetaWithID, node)
      })
    }).repartition(rddXmlMetaWithListNode.getNumPartitions)

    val rddMeta = rddXmlMetaWithNode.map({ case (meta, datiPod) =>

      val ITALIAN_DATE_PATTERN = "dd/MM/yyyy"
      val DB_DATE_PATTERN = "yyyy-MM-dd"
      val pod = (datiPod \\ Pod).text

      val dataMisuraDateTime = Try(DateTime.parse((datiPod \\ Montaggio \\ DataMisura).text, DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN))).getOrElse(DateTime.parse("31/12/9999", DateTimeFormat.forPattern(ITALIAN_DATE_PATTERN)))
      val originalFormat = DateTimeFormatter.ofPattern(DB_DATE_PATTERN)
      val dataMisura = LocalDate.parse(dataMisuraDateTime.toString(DB_DATE_PATTERN), originalFormat)

      ((pod, meta.pivaDistributore), (dataMisura, meta, datiPod))

    })

    val rddWithIsPodCompetenceDistrUdd = rddMeta
      .leftOuterJoin(resultId18)
      .map({ case ((pod, pivaDistributore), ((dataMisura, meta, datiPod), desc)) => {
        val isPodCompetenceDistr =
          if (desc.isDefined) {
            desc.get.exists(values => {
              val dataInizio = values._1
              val dataFine = values._2
              dataInizio.isEqual(dataMisura) || dataFine.isEqual(dataMisura) || (dataInizio.isBefore(dataMisura) && dataFine.isAfter(dataMisura))
            })
          } else false
        (pod, (dataMisura, meta.copy(isPodCompetenceDistr = isPodCompetenceDistr), datiPod))
      }
      })
      .leftOuterJoin(resultId19)
      .map({ case (pod, ((dataMisura, meta, datiPod), desc)) => {
        val isPodCompetenceUdd =
          if (desc.isDefined) {
            desc.get.exists(values => {
              val dataInizio = values._1
              val dataFine = values._2
              val codiceTerna = values._3
              (dataInizio.isEqual(dataMisura) || dataFine.isEqual(dataMisura) || (dataInizio.isBefore(dataMisura) && dataFine.isAfter(dataMisura))) && codiceTerna == meta.codDp
            })
          } else false
        (pod, (dataMisura, meta.copy(isPodCompetenceUdd = isPodCompetenceUdd), datiPod))
      }
      })
      .leftOuterJoin(rcuPodStatoForValidation)
      .map({ case (pod, ((dataMisura, meta, datiPod), desc)) => {
        val isPodActiveAtDataMontaggio =
          if (desc.isDefined) {
            desc.get.exists(values => {
              val dataInizio = values._1
              val dataFine = values._2
              val statoAttivazione = values._3
              (dataInizio.isEqual(dataMisura) || dataFine.isEqual(dataMisura) || (dataInizio.isBefore(dataMisura) && dataFine.isAfter(dataMisura))) && (statoAttivazione.isEmpty || statoAttivazione.get.equals("A"))
            })
          } else false
        (meta.copy(isPodActiveAtDataMontaggio = isPodActiveAtDataMontaggio), datiPod)
      }
      })

    val fileWithMessages = rddWithIsPodCompetenceDistrUdd.map({ case (fileXmlWithMeta, datiPod) =>
      //if is a smis flow then it use smis ammissibilita else it use original ammissibilita
      val checker: CheckAmmissibilitaRules[ReportEsitoPODMessage] = CheckAmmissibilitaPodRulesSMIS
      val messages = checker.check(datiPod, fileXmlWithMeta)
      (fileXmlWithMeta.file.getParent, (fileXmlWithMeta, messages))
    })
      .groupByKey()
      .map({ case (idXml, values) =>
        val (firstXml, firstMessages) = values.head
        (firstXml, values.map({ case (fileXmlWithMeta, messages) => messages }).toList)
      })
      .union(rddXmlMetaWithNodeIsEmpty)
      .coalesce(rddXmlMetaWithListNode.getNumPartitions)

    fileWithMessages.map({ case (meta, messageList) =>
      val newMessages = messageList.map(getMessageWithNewLogic(_).asInstanceOf[ReportEsitoPODMessage])
      (meta, newMessages)
    })

  }

  def writeOnHive(rdd: RDD[ReportEsitoPODMessage]): Unit = {
    val sc = Environment.getSpark.sparkContext
    val SQLContext = Environment.getSpark.sqlContext
    import SQLContext.implicits._

    val codice_inammissibilita = "codiceInamissibilita"
    val replaceEmptyStrWithNullExpression = when(col(codice_inammissibilita) =!= lit(""), col(codice_inammissibilita))
    rdd.toDF
      .withColumn(d_caricamento, current_timestamp)
      .withColumn(codice_inammissibilita, replaceEmptyStrWithNullExpression)
      .withColumnRenamed("cartellaCloud", AmmissibilitaPodSchema.cartella_cloud)
      .withColumnRenamed("nomeFile", AmmissibilitaPodSchema.nome_file)
      .withColumnRenamed("codiceInamissibilita", AmmissibilitaPodSchema.codice_inamissibilita)
      .repartition(100)
      .selectExpr(AmmissibilitaPodSchema.getValues: _*)
      .write
      //.partitionBy(anno, mese, giorno) --> /* COMMENTED IN ORDER TO FIX THIS: insertInto() can't be used together with partitionBy().
      //      //      // Partition columns have already be defined for the table. It is not necessary to use partitionBy() */
      //      //
      .insertInto(PropertyUtility.getAmmissibilitaPodLogTable)
  }

  def writeAggregatedPodTxtReport(fileWithMessages: RDD[(XMLMetadata, List[ReportEsitoPODMessage])], syncRoot: String, params: AmmissibilitaParameters): Unit = {
    val fileWithAggregatedMessages = fileWithMessages
      .map({ case (meta, listOfMessages) => (meta.file.getParentFile.getPath, listOfMessages) })
      .reduceByKey({ case (messageList1, messageList2) => messageList1 ++ messageList2 })

    fileWithAggregatedMessages.foreach({ case (parentFolder, messages) =>
      val originalPathArray = parentFolder.split(File.separatorChar)
      val syncSubpath = originalPathArray.slice(originalPathArray.length - 5, originalPathArray.length)
      val syncRootPath = syncRoot
      val outFolder: String = syncRootPath + s"${File.separator}" + syncSubpath.mkString(File.separator)
      //val reportTimestamp =  DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now())
      //if is a smis flow then it use smis output file else it use original output file
      val outputPathFile = if (params.isSmis) s"$outFolder/ReportEsitoPODSMIS.txt" else s"$outFolder/ReportEsitoPOD.txt"
      FileUtility.writeCsv(outputPathFile, ReportEsitoPODMessage.header, messages.map(_.toStringRow), Some(PropertyUtility.getAmmissibilitaPodFileMaxLength), appendMode = true)
    })

  }

  def getExceptionMessage(meta: XMLMetadata): ReportEsitoPODMessage = {
    ReportEsitoPODMessage(cartellaCloud = meta.file.getParent
      , nomeFile = meta.file.getName
      , flusso = meta.flusso
      , ammissibilita = Constants.FILE
      , bloccante = Constants.BLOCCANTE
      , codiceInamissibilita = Constants.COD904
      , descrizione = Constants.ERROR_FILE_STRUCTURE
    )
  }

  def recoverRddXmlWithMetaSMIS(rcuAziendaPDF: DataFrame, rcuPod: DataFrame, rcusPod: DataFrame, rcuPodDistr: DataFrame, rcusPodDistr: DataFrame, rcuPodUdd: DataFrame, rcusPodUdd: DataFrame, rcuUdd: DataFrame): (DataFrame, DataFrame) = {

    val unionRcuPodAndRcusPod = rcuPod.union(rcusPod).coalesce(rcuPod.rdd.getNumPartitions)
    //      .persist(StorageLevel.MEMORY_AND_DISK)

    val rddWithIsPodCompetenceDistr = getPodCompetenceDistrOrUdd(rcuAziendaPDF, unionRcuPodAndRcusPod, rcuPodDistr, rcusPodDistr, rcuUdd, rcuPodUdd, rcusPodUdd)
    //      .map({ case (pod, isPodCompetenceDistr, isPodCompetenceUdd, meta) => meta.copy(isPodCompetenceDistr = isPodCompetenceDistr, isPodCompetenceUdd = isPodCompetenceUdd) })

    rcuAziendaPDF.unpersist()
    unionRcuPodAndRcusPod.unpersist()

    rddWithIsPodCompetenceDistr
  }

  //competence is true if is Distr else if is Udd
  def getPodCompetenceDistrOrUdd(rcuAziendaPDF: DataFrame, unionRcuPodAndRcusPod: DataFrame, rcuPodDistr: DataFrame, rcusPodDistr: DataFrame, rcuUddPDF: DataFrame, rcuPodUdd: DataFrame, rcusPodUdd: DataFrame): (DataFrame, DataFrame) = {

    //    val rdd = rddMeta.map(meta => (meta.pod, (meta.dataMisura, meta)))

    val rcuUdd = rcuUddPDF.select(RcuUddPSchema.n_id_udd, RcuUddPSchema.t_codice_terna)

    unionRcuPodAndRcusPod.persist(StorageLevel.MEMORY_AND_DISK)

    //    log.info(s"Count di unionRcuPodAndRcusPod: " + unionRcuPodAndRcusPod.count)

    val rcuPodDistrJoinRcuAziendaPodPodId18 =
      rcuPodDistr
        .join(unionRcuPodAndRcusPod, rcuPodDistr(RcuPodDistrPSchema.n_id_pod) === unionRcuPodAndRcusPod(RcuPodPSchema.n_id_pod))
        .drop(rcuPodDistr(RcuPodDistrPSchema.n_id_pod))
        .join(rcuAziendaPDF, rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda) === rcuPodDistr(RcuPodDistrPSchema.n_id_distr))
        .select(
          unionRcuPodAndRcusPod(RcuPodPSchema.t_codice_pod),
          rcuAziendaPDF(RcuAziendaPSchema.t_piva),
          rcuPodDistr(RcuPodDistrPSchema.d_inizio),
          rcuPodDistr(RcuPodDistrPSchema.d_fine)
        )

    // rcuPodDistrJoinRcuAziendaPodPodId18.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // log.info(s"Count di rcuPodDistrJoinRcuAziendaPodPodId18: " + rcuPodDistrJoinRcuAziendaPodPodId18.filter(col("t_codice_pod") === "IT026E00004835").count)

    val rcusPodDistrJoinRcuAziendaPodPodId18 =
      rcusPodDistr
        .join(unionRcuPodAndRcusPod, rcusPodDistr(RcusPodDistrPSchema.n_id_pod) === unionRcuPodAndRcusPod(RcuPodPSchema.n_id_pod))
        .drop(rcusPodDistr(RcusPodDistrPSchema.n_id_pod))
        .join(rcuAziendaPDF, rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda) === rcusPodDistr(RcusPodDistrPSchema.n_id_distr))
        .select(
          unionRcuPodAndRcusPod(RcuPodPSchema.t_codice_pod),
          rcuAziendaPDF(RcuAziendaPSchema.t_piva),
          rcusPodDistr(RcusPodDistrPSchema.d_inizio),
          rcusPodDistr(RcusPodDistrPSchema.d_fine)
        )

    // rcusPodDistrJoinRcuAziendaPodPodId18.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // log.info(s"Count di rcusPodDistrJoinRcuAziendaPodPodId18: " + rcusPodDistrJoinRcuAziendaPodPodId18.filter(col("t_codice_pod") === "IT026E00004835").count)

    val unionPodDistrId18 = rcuPodDistrJoinRcuAziendaPodPodId18.union(rcusPodDistrJoinRcuAziendaPodPodId18).coalesce(rcusPodDistrJoinRcuAziendaPodPodId18.rdd.getNumPartitions)

    // unionPodDistrId18.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // log.info(s"Count di unionPodDistrId18: " + unionPodDistrId18.filter(col("t_codice_pod") === "IT026E00004835").count)


    // resultId18.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // log.info(s"Count di resultId18: " + resultId18.filter(col("t_codice_pod") === "IT026E00004835").count)

    val rcuPodDistrJoinRcuAziendaPodPodId19 =
      rcuPodUdd
        .join(unionRcuPodAndRcusPod, rcuPodUdd(RcuPodUddPSchema.n_id_pod) === unionRcuPodAndRcusPod(RcuPodPSchema.n_id_pod))
        .drop(rcuPodUdd(RcuPodUddPSchema.n_id_pod))
        .join(rcuAziendaPDF, rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda) === rcuPodUdd(RcuPodUddPSchema.n_id_udd))
        .join(rcuUdd, rcuUdd(RcuUddPSchema.n_id_udd) === rcuPodUdd(RcuPodUddPSchema.n_id_udd))
        .select(
          unionRcuPodAndRcusPod(RcuPodPSchema.t_codice_pod),
          //rcuAziendaPDF(RcuAziendaPSchema.t_piva),
          rcuPodUdd(RcuPodUddPSchema.d_inizio),
          rcuPodUdd(RcuPodUddPSchema.d_fine),
          rcuUdd(RcuUddPSchema.t_codice_terna)
        )

    //    rcuPodDistrJoinRcuAziendaPodPodId19.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    log.info(s"Count di rcuPodDistrJoinRcuAziendaPodPodId19: " + rcuPodDistrJoinRcuAziendaPodPodId19.count)

    val rcusPodDistrJoinRcuAziendaPodPodId19 =
      rcusPodUdd
        .join(unionRcuPodAndRcusPod, rcusPodUdd(RcusPodUddPSchema.n_id_pod) === unionRcuPodAndRcusPod(RcuPodPSchema.n_id_pod))
        .drop(rcusPodUdd(RcusPodUddPSchema.n_id_pod))
        .join(rcuAziendaPDF, rcuAziendaPDF(RcuAziendaPSchema.n_id_azienda) === rcusPodUdd(RcusPodUddPSchema.n_id_udd))
        .join(rcuUdd, rcuUdd(RcuUddPSchema.n_id_udd) === rcusPodUdd(RcusPodUddPSchema.n_id_udd))
        .select(
          unionRcuPodAndRcusPod(RcuPodPSchema.t_codice_pod),
          //rcuAziendaPDF(RcuAziendaPSchema.t_piva),
          rcusPodUdd(RcusPodUddPSchema.d_inizio),
          rcusPodUdd(RcusPodUddPSchema.d_fine),
          rcuUdd(RcuUddPSchema.t_codice_terna)
        )

    //    rcusPodDistrJoinRcuAziendaPodPodId19.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    log.info(s"Count di rcusPodDistrJoinRcuAziendaPodPodId19: " + rcusPodDistrJoinRcuAziendaPodPodId19.count)

    val unionPodDistrId19 = rcuPodDistrJoinRcuAziendaPodPodId19.union(rcusPodDistrJoinRcuAziendaPodPodId19).coalesce(rcusPodDistrJoinRcuAziendaPodPodId19.rdd.getNumPartitions)

    //    unionPodDistrId19.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    log.info(s"Count di unionPodDistrId19: " + unionPodDistrId19.count)


    //    resultId19.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    log.info(s"Count di resultId19: " + resultId19.count)


    (unionPodDistrId18, unionPodDistrId19)
  }
}
