package it.eng.au.ERP.flow

import it.eng.au.ERP.dao.hive.erp.{ErpAggregatoDao, ErpAggregatoPubDao, ErpDettaglioIntDao, ErpValidatedIntDao, ErpDetPodIntPubDao}
import it.eng.au.ERP.schema.erp.{erpAggregatoPubSchema, erpDettaglioINTSchema, erpValidatedIntSchema}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
import java.nio.charset.StandardCharsets
import java.util.zip.{ZipEntry, ZipOutputStream}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import it.eng.au.ERP.utility.environment.Environment
import it.eng.au.ERP.utility.functions.argumentsUtilities
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, max => smax, concat, concat_ws, trim, length, round, coalesce, sum, first, regexp_replace, lpad}
import scala.math.BigDecimal.RoundingMode
import scala.util.Try

/**
  * Messa a disposizione DATI
  *
  * Seleziona da ERP_AGGREGATO i record con ANNO/MESE richiesti,
  * individua il MAX(executionid) e filtra PIVA_DISTR != '05779661007'
  * (configurabile via proprietà), quindi scrive file CSV per ciascuna PIVA.
  *
  * Nota: i tracciati XML/CSV dettagliati [D01] saranno integrati in passi successivi;
  * questa prima versione produce CSV tabellari per PIVA a supporto della pubblicazione.
  */
case class MessaADisposizioneDATIFlow()(implicit spark: SparkSession) extends Flow {
  private val erpAggregatoDao = new ErpAggregatoDao
  private val erpAggregatoPubDao = new ErpAggregatoPubDao
  private val erpValidatedIntDao = new ErpValidatedIntDao
  private val erpDettaglioIntDao = new ErpDettaglioIntDao
  private val erpDetPodIntPubDao = new ErpDetPodIntPubDao

  def run(timestamp: Long, baseOutputOpt: Option[String], annomese: Option[String]): Unit = {

    val (yearOpt, monthOpt) = argumentsUtilities.yearMonth(annomese) match {
      case Some((y, m)) => (Some(y), Some(m))
      case None => (None, None)
    }

    if (yearOpt.isEmpty || monthOpt.isEmpty) {
      logger.warn("DATI: annomese non valorizzato; pubblicazione non eseguita")
      return
    }
    val year = yearOpt.get; val month = monthOpt.get

    val baseOutput = baseOutputOpt
      .orElse(Environment.getOptionalProperty("spark.app.energia_residuale_pariziale.paramentro.dati_output_path"))
      .orElse(Environment.getOptionalProperty("deploy.path.erp"))
      .getOrElse(throw new IllegalArgumentException("Path output DATI non fornito (--DATI o proprietà spark.app.energia_residuale_pariziale.paramentro.dati_output_path)"))

    val outputMount = Environment
      .getOptionalProperty("spark.app.energia_residuale_pariziale.paramentro.dati_output_mount")
      .map(_.trim.stripPrefix("/").stripSuffix("/"))
      .filter(_.nonEmpty)

    val baseOutputWithMount = outputMount match {
      case Some(subPath) => joinPaths(baseOutput, subPath)
      case None => baseOutput
    }

    // Parametri di pubblicazione
    val pubTipoRaw = Environment
      .getOptionalProperty("spark.app.energia_residuale_pariziale.paramentro.pub_tipo")
      .map(_.trim.toUpperCase)
      .getOrElse("AC4") // AC4 = Distributori, AC2 = Terna, AC4_INT = solo dettaglio INT

    // pubTipo: valore logico per aggregato (AC2/AC4)
    // onlyDettaglioInt: se true esegue solo il flusso INT (ZIP), senza AC4
    val (pubTipo, onlyDettaglioInt) = pubTipoRaw match {
      case "AC2"     => ("AC2", false)
      case "AC4_INT" => ("AC4", true)
      case _         => ("AC4", false)
    }

    // PIVA Terna configurabile; riusa default storico
    val pivaTerna = Environment
      .getOptionalProperty("spark.app.energia_residuale_pariziale.paramentro.piva_terna")
      .orElse(Environment.getOptionalProperty("spark.app.energia_residuale_pariziale.paramentro.piva_esclusa"))
      .getOrElse("05779661007")

    logger.info(s"DATI: lettura ${erpAggregatoDao.tableName} per anno=$year mese=$month")
    val df = erpAggregatoDao.read()
      .filter(col("anno") === lit(year) && col("mese") === lit(month))

    if (df.head(1).isEmpty) {
      logger.warn("DATI: nessun dato presente per l'annomese selezionato")
      return
    }

    // executionid è string in Hive; calcoliamo il max come long per evitare ClassCastException
    val maxExec = df
      .agg(smax(col("executionid").cast("long")).as("max_executionid"))
      .collect()(0)
      .getAs[Long](0)
    logger.info(s"DATI: max executionid identificato=$maxExec")

    val baseFiltered = df
      // confronta su cast long per uniformità con maxExec
      .filter(col("executionid").cast("long") === lit(maxExec))

    val filtered = pubTipo match {
      case "AC2" =>
        // Solo TERNA
        baseFiltered.filter(col("piva_distr") === lit(pivaTerna))
      case _ =>
        // AC4: tutti i distributori esclusa TERNA
        baseFiltered.filter(col("piva_distr") =!= lit(pivaTerna))
    }

    // Usa solo record con giorno valorizzato; nessun fallback su DATA
    val filteredValid = filtered.filter(col("giorno").isNotNull && trim(col("giorno")) =!= lit(""))

    if (filtered.head(1).isEmpty) {
      logger.warn("DATI: nessun dato dopo filtro executionid/PIVA esclusa")
      return
    }

    // Cartelle secondo specifica: /<base>/<AC*>/<ANNO>/<MESE>/<TIMESTAMP>
    val annoMeseStr = f"$year%04d$month%02d"
    val yearStr = f"$year%04d"
    val monthStr = f"$month%02d"
    val acFolder = pubTipo // "AC2" | "AC4"
    val runRoot = joinPaths(baseOutputWithMount, acFolder, yearStr, monthStr, timestamp.toString)
    val tsStr = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss").format(Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()))

    val outputLocation = resolveOutputLocation(runRoot)
    val directoryPath = ensureTrailingSlash(outputLocation.metadataPath)

    if (!onlyDettaglioInt) {
      val pivot = filteredValid.select("piva_distr").distinct.collect.map(_.getString(0))
      logger.info(s"DATI: numero PIVA da pubblicare: ${pivot.length}")

      // Scrittura su tabella PUB (contenuto + metadati file ripetuti per riga)
      val pathBase = directoryPath
      val pubCsvDf = {
        val qhCols = (1 to 100).map { i => round(col(s"q$i"), 0).cast("bigint").alias(s"qh$i") }
        val baseNameCol =
          if (pubTipo == "AC2")
            concat(lit(tsStr + "_AGGR_ERP_"), col("piva_distr"))
          else
            concat(lit(tsStr + "_AGGR_ERP_"), col("piva_distr"), lit("_" + annoMeseStr))
        val base = filteredValid
          .withColumn("data", col("giorno"))
          .withColumn("piva_gestore_rete", col("piva_distr"))
          .withColumn("ragione_sociale_gestore_rete", col("rag_soc_distr"))
          .withColumn("zona_mercato", col("area"))
          .withColumn("tipo_pubb", lit(pubTipo))
          .withColumn("nome_file", baseNameCol)
          .withColumn("tipo_file", lit("CSV"))
          .withColumn("path", lit(pathBase))
          .withColumn("executionid_in", lit(maxExec.toString))
          .withColumn("anno", col("anno").cast("string"))
          .withColumn("mese", col("mese").cast("string"))
          .withColumn("executionid", lit(timestamp))

        val cols: Seq[org.apache.spark.sql.Column] =
          Seq(
            col("data"),
            col("piva_gestore_rete"),
            col("ragione_sociale_gestore_rete"),
            col("zona_mercato")
          ) ++ qhCols ++ Seq(
            col("tipo_pubb"),
            col("nome_file"),
            col("tipo_file"),
            col("path"),
            col("executionid_in"),
            col("anno"),
            col("mese"),
            col("executionid")
          )

        base.select(cols: _*)
      }

      erpAggregatoPubDao.write(pubCsvDf, overwrite = false)

      val csvHeader = "ANNO;MESE;PIVA_GESTORE_RETE;RAGIONE_SOCIALE_GESTORE_RETE;DATA;ZONA_MERCATO;" +
        (1 to 100).map(i => s"QH$i").mkString(";")

      pivot.foreach { piva =>
        val desiredFileName = if (pubTipo == "AC2") s"${tsStr}_AGGR_ERP_${piva}.csv" else s"${tsStr}_AGGR_ERP_${piva}_${annoMeseStr}.csv"
        val dfPiva: DataFrame = filteredValid.filter(col("piva_distr") === lit(piva))
        val csvColumns: Seq[org.apache.spark.sql.Column] =
          Seq(
            col("anno").cast("string").alias("ANNO"),
            col("mese").cast("string").alias("MESE"),
            col("piva_distr").alias("PIVA_GESTORE_RETE"),
            col("rag_soc_distr").alias("RAGIONE_SOCIALE_GESTORE_RETE"),
            col("giorno").alias("DATA"),
            col("area").alias("ZONA_MERCATO")
          ) ++ (1 to 100).map(i => round(col(s"q$i"), 0).cast("long").alias(s"QH$i"))
        val dfCsv = dfPiva.select(csvColumns: _*)

        // Scrive direttamente un CSV unico senza staging, con header
        try {
          val fs = outputLocation.fs
          val outPath = new Path(outputLocation.fsPath, desiredFileName)
          val parent = outPath.getParent
          if (parent != null && !fs.exists(parent)) fs.mkdirs(parent)
          if (fs.exists(outPath)) fs.delete(outPath, false)
          val out = fs.create(outPath, true)
          val charset = StandardCharsets.UTF_8

          // Header
          val header = csvHeader + "\n"
          out.write(header.getBytes(charset))

          import scala.collection.JavaConverters._
          val iter = dfCsv.toLocalIterator().asScala
          iter.foreach { row =>
            val line = rowToCsv(row) + "\n"
            out.write(line.getBytes(charset))
          }
          out.close()
        } catch {
          case e: Throwable => logger.error(s"DATI: scrittura CSV fallita in cartella $runRoot: ${e.getMessage}")
        }
        logger.info(s"DATI: scritto CSV nella cartella $runRoot")

        // Genera XML conforme (senza validazione XSD) e salva con nomenclatura
        val xmlFileName = if (pubTipo == "AC2") s"${tsStr}_AGGR_ERP_${piva}.xml" else s"${tsStr}_AGGR_ERP_${piva}_${annoMeseStr}.xml"
        val xmlOutPath = new Path(outputLocation.fsPath, xmlFileName)
        try {
          val fs = outputLocation.fs
          val parent = xmlOutPath.getParent
          if (parent != null && !fs.exists(parent)) fs.mkdirs(parent)
          val detailCols: Seq[org.apache.spark.sql.Column] =
            Seq(
              col("giorno").alias("data"),
              col("area").alias("zona_mercato")
            ) ++ (1 to 100).map(i => col(s"q$i").alias(s"q$i"))

          val rows = dfPiva
            .select(detailCols: _*)
            .collect()

          // Recupera ragione sociale
          val ragSoc = dfPiva.select(col("rag_soc_distr")).limit(1).collect().headOption.map(_.getString(0)).getOrElse("")

          val sb = new StringBuilder()
          sb.append("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n")
          sb.append(s"""<ERP_MISURE_MASTER ANNO="$year" MESE="$month" PIVA_GESTORE_DI_RETE="$piva" RAGIONE_SOCIALE_GESTORE_DI_RETE="${escapeXml(ragSoc)}">
""")
          rows.foreach { r =>
            val data = r.getAs[String]("data")
            val zona = r.getAs[String]("zona_mercato")
            sb.append(s"""  <DETAIL DATA="$data" ZONA_MERCATO="$zona"""")
            (1 to 100).foreach { i =>
              val vAny = r.getAs[Any](s"q$i")
              val v = roundValueToLong(vAny)
              sb.append(s""" QH$i="$v"""")
            }
            sb.append("/>\n")
          }
          sb.append("</ERP_MISURE_MASTER>\n")

          val out = fs.create(xmlOutPath, true)
          out.write(sb.toString().getBytes("UTF-8"))
          out.close()
          logger.info(s"DATI: scritto XML nella cartella $runRoot")
        } catch {
          case e: Throwable => logger.error(s"DATI: scrittura XML fallita in cartella $runRoot: ${e.getMessage}")
        }
      }
    }

    publishDettaglioPodInterconnessione(year, month, tsStr, annoMeseStr, outputLocation, pubTipoRaw, timestamp, maxExec)

    logger.info("DATI: pubblicazione CSV per DISTRIBUTORE completata")
  }

  private def publishDettaglioPodInterconnessione(year: Int,
                                                  month: Int,
                                                  tsStr: String,
                                                  annoMeseStr: String,
                                                  outputLocation: OutputLocation,
                                                  pubTipoRaw: String,
                                                  timestamp: Long,
                                                  maxExec: Long): Unit = {
    // Il dettaglio POD Interconnessione viene eseguito solo in modalità AC4_INT
    if (pubTipoRaw != "AC4_INT") {
      logger.info("DATI: dettaglio POD Interconnessione previsto solo per AC4_INT, pubblicazione saltata")
      return
    }

    val validatedBase = erpValidatedIntDao.read()
      .filter(col(erpValidatedIntSchema.anno) === lit(year) && col(erpValidatedIntSchema.mese) === lit(month))

    if (validatedBase.head(1).isEmpty) {
      logger.warn("DATI: nessun dato disponibile su ERP_VALIDATED_INT per il dettaglio POD Interconnessione")
      return
    }

    val maxExecValidatedRow = validatedBase
      .agg(smax(col(erpValidatedIntSchema.executionid).cast("long")).as("max_executionid"))
      .collect()(0)
    val maxExecValidatedOpt = Option(maxExecValidatedRow.getAs[java.lang.Long](0)).map(_.longValue())
    if (maxExecValidatedOpt.isEmpty) {
      logger.warn("DATI: impossibile determinare executionid su ERP_VALIDATED_INT per il dettaglio POD Interconnessione")
      return
    }
    val maxExecValidated = maxExecValidatedOpt.get

    val validatedFiltered = validatedBase
      .filter(col(erpValidatedIntSchema.executionid).cast("long") === lit(maxExecValidated))

    val detailEnergy = computeMonthlyEnergy(validatedFiltered)

    if (detailEnergy.head(1).isEmpty) {
      logger.warn("DATI: nessun POD disponibile per il dettaglio Interconnessione dopo aggregazione VALIDATED_INT")
      return
    }

    // Write publication metadata rows for ERP_DET_POD_INT_PUB
    try {
      val dfPub = detailEnergy
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.annomese.toString,
          concat(lpad(col("anno").cast("string"), 4, "0"), lpad(col("mese").cast("string"), 2, "0")))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.codice_pod.toString, col("pod"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.tensione.toString, col("tensione").cast("string"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.energia_immessa.toString, round(col("energia_immessa"), 3).cast("string"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.energia_prelevata.toString, round(col("energia_prelevata"), 3).cast("string"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.tipo_pubb.toString, lit("AC4"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.piva_distr.toString, col("piva_distr"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.area.toString, col("area"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.nome_file.toString,
          concat(col("piva_distr"), lit(s"_DETTAGLIOPOD_INT_${annoMeseStr}.zip")))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.path.toString, lit(outputLocation.fsPath))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.executionid_in.toString, lit(maxExec.toString))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.anno.toString, col("anno").cast("string"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.mese.toString, col("mese").cast("string"))
        .withColumn(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.executionid.toString, lit(timestamp))

      val dfPubOrdered = dfPub.select(it.eng.au.ERP.schema.erp.erpDetPodIntPubSchema.getValues.map(col): _*)
      erpDetPodIntPubDao.write(dfPubOrdered, overwrite = false)
      logger.info("DATI: scritto metadati pubblicazione su ERP_DET_POD_INT_PUB")
    } catch {
      case e: Throwable => logger.error(s"DATI: scrittura metadati su ERP_DET_POD_INT_PUB fallita: ${e.getMessage}")
    }
    writeDettaglioZip(detailEnergy, annoMeseStr, tsStr, outputLocation)
  }

  private def computeMonthlyEnergy(df: DataFrame): DataFrame = {
    val energiaPrelievoCols = sortedColumns(erpValidatedIntSchema.getValues, "ea_e")
    val energiaImmissioneCols = sortedColumns(erpValidatedIntSchema.getValues, "eaint_e")

    if (energiaPrelievoCols.isEmpty || energiaImmissioneCols.isEmpty) {
      throw new IllegalStateException("DATI: colonne energia INT non trovate per il dettaglio POD Interconnessione")
    }

    // Calcolo NETTO (al netto delle perdite di rete) - mantenuto commentato nel caso si voglia ripristinare:
    // val coeffPrelievo = toDoubleColumn(col(erpValidatedIntSchema.ka)) * toDoubleColumn(col(erpValidatedIntSchema.coefficienteperditaprel))
    // val coeffImmissione = toDoubleColumn(col(erpValidatedIntSchema.ka)) * toDoubleColumn(col(erpValidatedIntSchema.coefficienteperditaimm))
    // val dfWithTotalsNet = df
    //   .withColumn("energia_prelevata_row", sumColumns(energiaPrelievoCols) * coeffPrelievo)
    //   .withColumn("energia_immessa_row", sumColumns(energiaImmissioneCols) * coeffImmissione)

    // Calcolo LORDO: somma mensile delle curve EA_E* / EAINT_E* senza applicare KA e coefficienti di perdita
    val dfWithTotals = df
      .withColumn("energia_prelevata_row", sumColumns(energiaPrelievoCols))
      .withColumn("energia_immessa_row", sumColumns(energiaImmissioneCols))

    dfWithTotals.groupBy(
      col(erpValidatedIntSchema.piva_distr_dest).alias("piva_distr"),
      col(erpValidatedIntSchema.area).alias("area"),
      col(erpValidatedIntSchema.pod).alias("pod"),
      col(erpValidatedIntSchema.tensione).alias("tensione")
    ).agg(
      sum(col("energia_immessa_row")).alias("energia_immessa"),
      sum(col("energia_prelevata_row")).alias("energia_prelevata"),
      first(col(erpValidatedIntSchema.anno)).alias("anno"),
      first(col(erpValidatedIntSchema.mese)).alias("mese")
    )
  }

  private def writeDettaglioZip(detailDf: DataFrame,
                                annoMeseStr: String,
                                tsStr: String,
                                outputLocation: OutputLocation): Unit = {
    val collectedRows = detailDf.select(
      col("piva_distr"),
      col("area"),
      col("pod"),
      col("tensione"),
      col("energia_immessa"),
      col("energia_prelevata"),
      col("anno"),
      col("mese")
    ).collect()

    if (collectedRows.isEmpty) {
      logger.warn("DATI: nessuna riga disponibile per lo ZIP dettaglio POD Interconnessione")
      return
    }

    val rowsByPiva = collectedRows.groupBy(_.getAs[String]("piva_distr"))
    rowsByPiva.foreach { case (piva, records) =>
      val zipFileName = s"${piva}_DETTAGLIOPOD_INT_${annoMeseStr}.zip"
      val zipPath = new Path(outputLocation.fsPath, zipFileName)
      if (outputLocation.fs.exists(zipPath)) outputLocation.fs.delete(zipPath, false)
      val zipStream = new ZipOutputStream(outputLocation.fs.create(zipPath, true))
      try {
        val recordsByArea = records.groupBy(row => Option(row.getAs[String]("area")).getOrElse("NA"))
        recordsByArea.toSeq.sortBy(_._1).foreach { case (areaValue, areaRows) =>
          val csvFileName = s"${piva}_${areaValue}_${annoMeseStr}_${tsStr}_001.csv"
          val sb = new StringBuilder()
          sb.append("CODICE_POD;TENSIONE;ENERGIA_IMMESSA;ENERGIA_PRELEVATA;ANNOMESE\n")
          areaRows.sortBy(_.getAs[String]("pod")).foreach { row =>
            val pod = row.getAs[String]("pod")
            val tensione = Option(row.getAs[Any]("tensione")).map(_.toString).getOrElse("")
            val energiaImmessa = formatDecimal(Option(row.getAs[java.lang.Double]("energia_immessa")).map(_.doubleValue()).getOrElse(0d))
            val energiaPrelevata = formatDecimal(Option(row.getAs[java.lang.Double]("energia_prelevata")).map(_.doubleValue()).getOrElse(0d))
            val anno = Option(row.getAs[Any]("anno")).map(_.toString.toInt).getOrElse(0)
            val mese = Option(row.getAs[Any]("mese")).map(_.toString.toInt).getOrElse(0)
            val annomese = f"$anno%04d$mese%02d"
            sb.append(Seq(pod, tensione, energiaImmessa, energiaPrelevata, annomese).mkString(";"))
            sb.append("\n")
          }
          zipStream.putNextEntry(new ZipEntry(csvFileName))
          zipStream.write(sb.toString().getBytes(StandardCharsets.UTF_8))
          zipStream.closeEntry()
        }
        logger.info(s"DATI: scritto ZIP dettaglio POD Interconnessione per PIVA=$piva nella cartella ${outputLocation.fsPath}")
      } catch {
        case e: Throwable =>
          logger.error(s"DATI: scrittura ZIP dettaglio POD Interconnessione fallita per PIVA=$piva: ${e.getMessage}")
      } finally {
        zipStream.close()
      }
    }
  }

  private def sortedColumns(values: Seq[String], prefix: String): Seq[String] =
    values.flatMap { value =>
      if (value.startsWith(prefix)) {
        val suffix = value.substring(prefix.length)
        if (suffix.nonEmpty && suffix.forall(_.isDigit)) {
          Some((suffix.toInt, value))
        } else None
      } else None
    }.sortBy(_._1).map(_._2)

  private def toDoubleColumn(c: org.apache.spark.sql.Column): org.apache.spark.sql.Column =
    regexp_replace(c.cast("string"), ",", ".").cast("double")

  private def sumColumns(columns: Seq[String]): org.apache.spark.sql.Column = {
    if (columns.isEmpty) {
      lit(0d)
    } else {
      columns.tail.foldLeft(coalesce(toDoubleColumn(col(columns.head)), lit(0d))) { (acc, name) =>
        acc + coalesce(toDoubleColumn(col(name)), lit(0d))
      }
    }
  }

  private def formatDecimal(value: Double): String =
    BigDecimal(value).setScale(3, RoundingMode.HALF_UP).toString()

  private case class OutputLocation(fs: FileSystem, fsPath: String, metadataPath: String)

  private def resolveOutputLocation(runRoot: String): OutputLocation = {
    val trimmed = runRoot.trim
    val conf = Environment.getSpark.sparkContext.hadoopConfiguration
    val path = new Path(trimmed)
    val uri = path.toUri
    val maybeScheme = Option(uri.getScheme)
    maybeScheme match {
      case Some("file") =>
        val metaPath = Option(uri.getPath).getOrElse(trimmed)
        OutputLocation(FileSystem.getLocal(conf), trimmed, metaPath)
      case Some(_) =>
        val fs = path.getFileSystem(conf)
        OutputLocation(fs, trimmed, trimmed)
      case None =>
        val isAbsoluteLocalPath = new File(trimmed).isAbsolute
        if (isAbsoluteLocalPath) {
          OutputLocation(FileSystem.getLocal(conf), trimmed, trimmed)
        } else {
          val deployPath = Environment.getOptionalProperty("deploy.path.erp")
            .map(_.trim)
            .filter(_.nonEmpty)
          val useLocal = deployPath.exists(trimmed.startsWith)
          if (useLocal) {
            OutputLocation(FileSystem.getLocal(conf), trimmed, trimmed)
          } else {
            // Fallback HDFS behavior disabilitato su richiesta (non rimuovere definitivamente)
            // OutputLocation(Environment.getFs, trimmed, trimmed)
            OutputLocation(FileSystem.getLocal(conf), trimmed, trimmed)
          }
        }
    }
  }

  private def joinPaths(base: String, segments: String*): String = {
    val cleanedBase = Option(base).map(_.trim).getOrElse("")
    segments.foldLeft(cleanedBase) { (acc, rawSegment) =>
      val segment = Option(rawSegment).map(_.trim).filter(_.nonEmpty).getOrElse("")
      if (segment.isEmpty) acc
      else if (acc.isEmpty) new Path(segment).toString
      else new Path(acc, segment).toString
    }
  }

  private def ensureTrailingSlash(path: String): String = {
    if (path == null || path.isEmpty) path
    else if (path.endsWith("/")) path
    else path + "/"
  }

  private def roundValueToLong(value: Any): Long = value match {
    case null => 0L
    case n: java.lang.Number =>
      roundBigDecimal(BigDecimal(n.toString))
    case s: String =>
      val trimmed = s.trim
      if (trimmed.isEmpty) 0L
      else Try(BigDecimal(trimmed)).map(roundBigDecimal).getOrElse(0L)
    case other =>
      Try(BigDecimal(other.toString)).map(roundBigDecimal).getOrElse(0L)
  }

  private def roundBigDecimal(bd: BigDecimal): Long =
    bd.setScale(0, RoundingMode.HALF_UP).longValue()

  private def escapeXml(s: String): String =
    Option(s).map(_.flatMap {
      case '&' => "&amp;"
      case '"' => "&quot;"
      case '\'' => "&apos;"
      case '<' => "&lt;"
      case '>' => "&gt;"
      case c => String.valueOf(c)
    }).getOrElse("")

  private def escapeCsvField(v: Any): String = {
    val s = Option(v).map(_.toString).getOrElse("")
    val needsQuote = s.exists(ch => ch == ';' || ch == '"' || ch == '\n' || ch == '\r')
    val esc = s.replace("\"", "\"\"")
    if (needsQuote) "\"" + esc + "\"" else esc
  }

  private def rowToCsv(r: Row): String = r.toSeq.map(escapeCsvField).mkString(";")
}




