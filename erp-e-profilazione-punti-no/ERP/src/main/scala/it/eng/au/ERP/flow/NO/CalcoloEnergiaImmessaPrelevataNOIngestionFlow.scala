package it.eng.au.ERP.flow.NO

import it.eng.au.ERP.dao.hive.au.{FlussiTecniciDao, FlussoMisureNoAggrDao, FlussoMisureSmisDao}
import it.eng.au.ERP.dao.hive.erp.ErpValidatedMisNODao
import it.eng.au.ERP.dao.hive.tratt_pod.TrattPodllAnnomesePartitionedDao
import it.eng.au.ERP.flow.Flow
import it.eng.au.ERP.schema.au.{flussiTeniciSchema, flussoMisureNoAggrSchema, flussoMisureSmisSchema}
import it.eng.au.ERP.schema.erp.erpValidatedMisNoSchema
import it.eng.au.ERP.schema.tratt_pod.trattPodAllAnnomesePartitionedSchema
import it.eng.au.ERP.trasformations.NO.{CalcoloPrelevatoPuntiPrelievoMisNOTrasformation, CalcoloPrelevatoPuntiPrelievoNonOrari}
import it.eng.au.ERP.utility.functions.argumentsUtilities
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

case class CalcoloEnergiaImmessaPrelevataNOIngestionFlow(implicit spark: SparkSession) extends Flow {

  //input dao
  val flussoMisureNoAggrDao = new FlussoMisureNoAggrDao
  val flussiTecniciDao = new FlussiTecniciDao
  val flussoMisureSmisDao = new FlussoMisureSmisDao
  val trattPodllAnnomesePartitionedDao = new TrattPodllAnnomesePartitionedDao

  // output dao
  val erpValidatedMisNODao = new ErpValidatedMisNODao

  // Consolidation: deduplicate validated NO rows per execution
  // Rule:
  //  - For each (pod, data_misura): keep only 1 record with max(time_stamp)
  //  - Exception: if and only if the group has exactly two records and they are SMIS_A and SMIS_B,
  //    keep both, selecting max(time_stamp) within each SMIS tipo.
  private def dedupValidatedMisNo(df: DataFrame): DataFrame = {
    val tipo = upper(trim(col(erpValidatedMisNoSchema.tipo_flusso)))
    val isSmisA = tipo === lit("SMIS_A")
    val isSmisB = tipo === lit("SMIS_B")

    val keyCols = Seq(
      col(erpValidatedMisNoSchema.pod),
      col(erpValidatedMisNoSchema.data_misura)
    )
    val keyNames = Seq(
      erpValidatedMisNoSchema.pod.toString,
      erpValidatedMisNoSchema.data_misura.toString
    )

    // Per-key counts to detect the special SMIS_A+SMIS_B case (exactly two rows)
    val perKey = df
      .withColumn("is_a", isSmisA.cast("int"))
      .withColumn("is_b", isSmisB.cast("int"))
      .groupBy(keyCols: _*)
      .agg(
        count(lit(1)).alias("cnt_all"),
        sum(col("is_a")).alias("cnt_a"),
        sum(col("is_b")).alias("cnt_b")
      )
      .withColumn("keep_both", col("cnt_all") === lit(2) && col("cnt_a") === lit(1) && col("cnt_b") === lit(1))

    // Join using column names (Spark join signature requires Seq[String] for usingColumns)
    val withKeyInfo = df.join(perKey, keyNames, "left")

    // Top-1 per (pod, data_misura) by time_stamp desc
    val wTop1 = Window.partitionBy(keyCols: _*)
      .orderBy(col(erpValidatedMisNoSchema.time_stamp).cast("string").desc_nulls_last)

    val top1All = withKeyInfo
      .withColumn("rn_all", row_number().over(wTop1))
      .filter(col("rn_all") === 1 && coalesce(col("keep_both"), lit(false)) === lit(false))

    // For special groups keep both SMIS_A and SMIS_B, taking top-1 per tipo
    val wPerTipo = Window.partitionBy(
      col(erpValidatedMisNoSchema.pod),
      col(erpValidatedMisNoSchema.data_misura),
      col(erpValidatedMisNoSchema.tipo_flusso)
    ).orderBy(col(erpValidatedMisNoSchema.time_stamp).cast("string").desc_nulls_last)

    val smisBoth = withKeyInfo
      .filter(col("keep_both") === lit(true) && (isSmisA || isSmisB))
      .withColumn("rn_tipo", row_number().over(wPerTipo))
      .filter(col("rn_tipo") === 1)

    val payloadCols = erpValidatedMisNoSchema.getValues
    top1All.selectExpr(payloadCols: _*)
      .unionByName(smisBoth.selectExpr(payloadCols: _*))
  }

  private val statsEnabled = false
  private def logDateStats(label: String, df: org.apache.spark.sql.DataFrame, column: String): Unit = {
    if (!statsEnabled) return
    val statsRow = df
      .agg(
        count(lit(1)).alias("total"),
        sum(when(col(column).isNull, lit(1)).otherwise(lit(0))).alias("nulls"),
        min(col(column)).alias("min"),
        max(col(column)).alias("max")
      )
      .head()

    val total = statsRow.getAs[Long]("total")
    val nulls = statsRow.getAs[Long]("nulls")
    val minVal = statsRow.getAs[Any]("min")
    val maxVal = statsRow.getAs[Any]("max")

    logger.info(s"$label stats -> total: $total, nulls: $nulls, min: $minVal, max: $maxVal")
  }

  private def normalizeDataMisura(df: DataFrame): DataFrame = {
    val baseCandidates = Seq(
      to_date(col(erpValidatedMisNoSchema.data_misura), "yyyy-MM-dd"),
      to_date(col(erpValidatedMisNoSchema.data_misura), "dd/MM/yyyy"),
      to_date(col(erpValidatedMisNoSchema.data_misura), "yyyyMMdd")
    )

    val fromYyyymmdd =
      if (df.columns.contains("data_misura_yyyymmdd"))
        Seq(to_date(col("data_misura_yyyymmdd"), "yyyyMMdd"))
      else Seq.empty

    val fromSource =
      if (df.columns.contains(flussoMisureNoAggrSchema.data_misura.toString))
        Seq(to_date(col(flussoMisureNoAggrSchema.data_misura.toString), "yyyy-MM-dd"))
      else Seq.empty

    val normalizedDate = (baseCandidates ++ fromYyyymmdd ++ fromSource)
      .reduceOption((a, b) => coalesce(a, b))
      .getOrElse(lit(null).cast("date"))

    df.withColumn(
      erpValidatedMisNoSchema.data_misura,
      date_format(normalizedDate, "yyyy-MM-dd")
    )
  }


  //def run
  def run(timestamp: Long, podExcluded: List[String], annomese: Option[String]
         ): Unit = {

    val (year, month)
    : (Option[Int], Option[Int]) = argumentsUtilities.yearMonth(annomese) match {
      case Some((year, month)) => (Some(year), Some(month))
      case None => (None, None)
    }

    val start_in = argumentsUtilities.startInFunction(annomese)
    val stop_in = argumentsUtilities.stopInFunction(annomese)

    

    //todo move pod filter after set anno and mese
    
    // ⚠️ DEBUG FILTER - Commentato per run completa ⚠️
    // Decommentare per velocizzare il debug su POD specifici
    /*
    val debugPodList = List(
      "IT020E00204942",  // ✅ RISOLTO: PNO2G mancante sponda sinistra (join_mese fix)
      "IT018E00749251",  // Possibili duplicati PNO2G
      "IT163E00078325",  // Errata associazione SMIS_B misura successiva
      "IT001E04533765"   // POD di test generale
    )
    */
    
    val flussoMisureNoAggr = flussoMisureNoAggrDao.read()
      //.filter(col(flussoMisureNoAggrSchema.pod).isin(debugPodList: _*))  // ⚠️ DEBUG ONLY - COMMENTATO
      .withColumn(
        "data_misura_date",
        coalesce(
          to_date(col(flussoMisureNoAggrSchema.data_misura), "dd/MM/yyyy"),
          //to_date(col(flussoMisureNoAggrSchema.annomesegiornodir).cast("string"), "yyyyMMdd"),
          to_date(col(flussoMisureNoAggrSchema.data_misura), "yyyy-MM-dd")
        )
      )
      .withColumn(
        flussoMisureNoAggrSchema.data_misura,
        date_format(col("data_misura_date"), "yyyy-MM-dd")
      )
      .withColumn(
        "data_misura_yyyymmdd",
        date_format(col("data_misura_date"), "yyyyMMdd")
      )
      .drop("data_misura_date")
      .repartition(col(flussoMisureNoAggrSchema.pod))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //          .filter(!(col(flussoMisureNoAggrSchema.pod).isin(podExcluded:_*)))
    
    // Removed heavy count() used only for debug
    logger.info(s"Cached flussoMisureNoAggr")
    // logDateStats("flussoMisureNoAggr.data_misura", flussoMisureNoAggr, flussoMisureNoAggrSchema.data_misura)

    val trattPodllAnnomesePartitioned = trattPodllAnnomesePartitionedDao.read()
      // 🎯 RIMOSSO partition pruning: Segmento Sinistro/Destro possono includere mesi adiacenti
      // Se il POD ha data_misura del mese precedente (es. 31/08) ma runni per 202509,
      // serve comunque tratt_pod di agosto per la JOIN
      //.filter(col(trattPodAllAnnomesePartitionedSchema.pod14).isin(debugPodList: _*))  // ⚠️ DEBUG ONLY - COMMENTATO
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //      .filter(!(col(trattPodAllAnnomesePartitionedSchema.pod14).isin(podExcluded:_*)))
    
    // Removed heavy count() used only for debug
    logger.info(s"Cached trattPodllAnnomesePartitioned")

    val flussiTecnici = flussiTecniciDao.read()
      //.filter(col(flussiTeniciSchema.pod).isin(debugPodList: _*))  // ⚠️ DEBUG ONLY - COMMENTATO
      .withColumn(
        "data_misura_date",
        coalesce(
          to_date(col(flussiTeniciSchema.data_misura), "yyyy-MM-dd"),
          to_date(col(flussiTeniciSchema.data_misura), "dd/MM/yyyy")
        )
      )
      .withColumn(
        flussiTeniciSchema.data_misura,
        date_format(col("data_misura_date"), "yyyy-MM-dd")
      )
      .withColumn(
        "data_misura_yyyymmdd",
        date_format(col("data_misura_date"), "yyyyMMdd")
      )
      // 🔧 FIX AV2G/DS2G: Crea anno e mese da data_misura_date per la JOIN con trattPod
      .withColumn(
        flussiTeniciSchema.anno,
        org.apache.spark.sql.functions.year(col("data_misura_date"))
      )
      .withColumn(
        flussiTeniciSchema.mese,
        lpad(org.apache.spark.sql.functions.month(col("data_misura_date")).cast("string"), 2, "0")
      )
      .drop("data_misura_date")
      .repartition(col(flussiTeniciSchema.pod))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //      .filter(!(col(flussiTeniciSchema.pod).isin(podExcluded:_*)))
    
    // Removed heavy count() used only for debug
    logger.info(s"Cached flussiTecnici")
    // logDateStats("flussiTecnici.data_misura", flussiTecnici, flussiTeniciSchema.data_misura)

    val flussoMisureSmis = flussoMisureSmisDao.read()
      //.filter(col(flussoMisureSmisSchema.pod).isin(debugPodList: _*))  // DEBUG: ONLY - COMMENTATO
      .withColumn(
        "data_misura_smn_date",
        coalesce(
          to_date(col(flussoMisureSmisSchema.data_misura_smn), "yyyy-MM-dd"),
          to_date(col(flussoMisureSmisSchema.data_misura_smn), "dd/MM/yyyy")
        )
      )
      .withColumn(
        "data_misura_mn_date",
        coalesce(
          to_date(col(flussoMisureSmisSchema.data_misura_mn), "yyyy-MM-dd"),
          to_date(col(flussoMisureSmisSchema.data_misura_mn), "dd/MM/yyyy")
        )
      )
      .withColumn(
        flussoMisureSmisSchema.data_misura_smn,
        date_format(col("data_misura_smn_date"), "yyyy-MM-dd")
      )
      .withColumn(
        "data_misura_smn_yyyymmdd",
        date_format(col("data_misura_smn_date"), "yyyyMMdd")
      )
      .withColumn(
        flussoMisureSmisSchema.data_misura_mn,
        date_format(col("data_misura_mn_date"), "yyyy-MM-dd")
      )
      .withColumn(
        "data_misura_mn_yyyymmdd",
        date_format(col("data_misura_mn_date"), "yyyyMMdd")
      )
      .drop("data_misura_smn_date", "data_misura_mn_date")
      .repartition(col(flussoMisureSmisSchema.pod))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //      .filter(!(col(flussoMisureSmisSchema.pod).isin(podExcluded:_*)))
    
    // Removed heavy count() used only for debug
    logger.info(s"Cached flussoMisureSmis")
    // logDateStats("flussoMisureSmis.data_misura_smn", flussoMisureSmis, flussoMisureSmisSchema.data_misura_smn)
    // logDateStats("flussoMisureSmis.data_misura_mn", flussoMisureSmis, flussoMisureSmisSchema.data_misura_mn)

    //flussi periodici - sponda sinistra

    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra segmento")
    
    val flussoMisureNoAggrSegmentoSinistra = CalcoloPrelevatoPuntiPrelievoNonOrari
      .flussiPeriodiciSegmentoSinistro(flussoMisureNoAggr, year, month, podExcluded)
      .persist()

    val trattPodllAnnomesePartitionedSegmentoSinistro = CalcoloPrelevatoPuntiPrelievoNonOrari
      .trattPodllAnnomesePartitionedPreparedSegmentoSinistro(trattPodllAnnomesePartitioned, year, month, podExcluded)
      .persist()

    val trattPodllAnnomesePartitionedSegmentoSinistroJoin =
      CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
        .prepareTrattPodForJoin(trattPodllAnnomesePartitionedSegmentoSinistro)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val trattPodllAnnomesePartitionedSegmentoSinistroBr =
      broadcast(trattPodllAnnomesePartitionedSegmentoSinistroJoin)

    val flussiPriodiciSegmentoSinistro = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciSpondaSinistraSegmentoFlusso1(
        flussoMisureNoAggrSegmentoSinistra,
        trattPodllAnnomesePartitionedSegmentoSinistroBr,
        start_in,
        stop_in,
        timestamp
      )
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra segmento")


    // Accumulo dataset validated; scrittura unica con dedup a fine run
    val validatedParts = scala.collection.mutable.ArrayBuffer[DataFrame]()
    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra segmento (buffer)")
    val flussiPriodiciSegmentoSinistroNormalized = normalizeDataMisura(flussiPriodiciSegmentoSinistro)
    validatedParts += flussiPriodiciSegmentoSinistroNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra segmento (buffer)")
    // logDateStats("erp_validated_mis_no write (periodici sinistra)", flussiPriodiciSegmentoSinistroNormalized, erpValidatedMisNoSchema.data_misura)


    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Periodici Sponda  destra segmento")
    //flussi periodici - sponda destra
    val flussoMisureNoAggrSegmentoDestra = CalcoloPrelevatoPuntiPrelievoNonOrari
      .flussiPeriodiciSegmentoDestro(flussoMisureNoAggr, year, month, podExcluded)
      .persist()

    val trattPodllAnnomesePartitionedSegmentoDestro = CalcoloPrelevatoPuntiPrelievoNonOrari
      .trattPodllAnnomesePartitionedPreparedSegmentoDestro(trattPodllAnnomesePartitioned, year, month, podExcluded)
      .persist()

    val trattPodllAnnomesePartitionedSegmentoDestroJoin =
      CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
        .prepareTrattPodForJoin(trattPodllAnnomesePartitionedSegmentoDestro)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val trattPodllAnnomesePartitionedSegmentoDestroBr =
      broadcast(trattPodllAnnomesePartitionedSegmentoDestroJoin)

    val flussiPriodiciSegmentoDestro = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciSpondaDestraSegmentoFlusso1(
        flussoMisureNoAggrSegmentoDestra,
        trattPodllAnnomesePartitionedSegmentoDestroBr,
        start_in,
        stop_in,
        timestamp
      )
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra segmento")

    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra segmento (buffer)")
    val flussiPriodiciSegmentoDestroNormalized = normalizeDataMisura(flussiPriodiciSegmentoDestro)
    validatedParts += flussiPriodiciSegmentoDestroNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra segmento (buffer)")
    // logDateStats("erp_validated_mis_no write (periodici destra)", flussiPriodiciSegmentoDestroNormalized, erpValidatedMisNoSchema.data_misura)

    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra destra segmento")

    //flussi periodici - sponda sinistra & destra
    val flussoMisureNoAggrSegmentoSinistraEDestra = CalcoloPrelevatoPuntiPrelievoNonOrari
      .flussiPeriodiciSegmentoSinistraEDestro(flussoMisureNoAggr, year, month, podExcluded)
      .persist()

    val trattPodllAnnomesePartitionedSegmentoSinistraEDestra = CalcoloPrelevatoPuntiPrelievoNonOrari
      .trattPodllAnnomesePartitionedPreparedSegmentoSinistroEDestro(trattPodllAnnomesePartitioned, year, month, podExcluded)
      .persist()

    val trattPodllAnnomesePartitionedSegmentoSinistraEDestraJoin =
      CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
        .prepareTrattPodForJoin(trattPodllAnnomesePartitionedSegmentoSinistraEDestra)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val trattPodllAnnomesePartitionedSegmentoSinistraEDestraBr =
      broadcast(trattPodllAnnomesePartitionedSegmentoSinistraEDestraJoin)

    val flussiPriodiciSegmentoSinistraEDestro = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiPeriodiciFlusso2(
        flussoMisureNoAggrSegmentoSinistraEDestra,
        trattPodllAnnomesePartitionedSegmentoSinistraEDestraBr,
        start_in,
        stop_in,
        timestamp
      )
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra destra segmento")

    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra destra segmento (buffer)")
    val flussiPriodiciSegmentoSinistraEDestroNormalized = normalizeDataMisura(flussiPriodiciSegmentoSinistraEDestro)
    validatedParts += flussiPriodiciSegmentoSinistraEDestroNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Periodici Sponda sinistra destra segmento (buffer)")
    // logDateStats("erp_validated_mis_no write (periodici sinistra+destra)", flussiPriodiciSegmentoSinistraEDestroNormalized, erpValidatedMisNoSchema.data_misura)

    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda sinistra segmento")
    //flusso rettifica Sponda Segmento Sinistro

    val flussoRettificaSegmentoSinistra = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiRettificaSegmentoSinistro(
        flussoMisureNoAggrSegmentoSinistra,
        trattPodllAnnomesePartitionedSegmentoSinistroBr,
        start_in,
        timestamp
      )

    flussoMisureNoAggrSegmentoSinistra.unpersist(blocking = false)
    trattPodllAnnomesePartitionedSegmentoSinistroJoin.unpersist(blocking = false)
    trattPodllAnnomesePartitionedSegmentoSinistro.unpersist(blocking = false)
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda sinistra segmento")

    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda sinistra segmento (buffer)")
    val flussoRettificaSegmentoSinistraNormalized = normalizeDataMisura(flussoRettificaSegmentoSinistra)
    validatedParts += flussoRettificaSegmentoSinistraNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda sinistra segmento (buffer)")
    // logDateStats("erp_validated_mis_no write (rettifica sinistra)", flussoRettificaSegmentoSinistraNormalized, erpValidatedMisNoSchema.data_misura)


    //flusso rettifica Sponda Segmento Destra

    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda destra segmento")
    val flussoRettificaSegmentoDestra = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiRettificaSegmentoDestro(
        flussoMisureNoAggrSegmentoDestra,
        trattPodllAnnomesePartitionedSegmentoDestroBr,
        stop_in,
        timestamp
      )

    flussoMisureNoAggrSegmentoDestra.unpersist(blocking = false)
    trattPodllAnnomesePartitionedSegmentoDestroJoin.unpersist(blocking = false)
    trattPodllAnnomesePartitionedSegmentoDestro.unpersist(blocking = false)
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda destra segmento")

    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda destra segmento (buffer)")
    val flussoRettificaSegmentoDestraNormalized = normalizeDataMisura(flussoRettificaSegmentoDestra)
    validatedParts += flussoRettificaSegmentoDestraNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Rettifica Sponda destra segmento (buffer)")
    // logDateStats("erp_validated_mis_no write (rettifica destra)", flussoRettificaSegmentoDestraNormalized, erpValidatedMisNoSchema.data_misura)

    //flussi tencnici
    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Tecnici")
    val dfFlussiTecniciSegmentoDestraESinistra = CalcoloPrelevatoPuntiPrelievoNonOrari
      .flussiTecniciSegmentoSinistraEDestro(flussiTecnici, year, month, podExcluded)

    val dfFlussiTecnici = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiTecnici(
        dfFlussiTecniciSegmentoDestraESinistra
        , trattPodllAnnomesePartitionedSegmentoSinistraEDestraBr
        , start_in
        , stop_in
        , timestamp
      )
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Tecnici")

    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Tecnici (buffer)")
    val dfFlussiTecniciNormalized = normalizeDataMisura(dfFlussiTecnici)
    validatedParts += dfFlussiTecniciNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Tecnici (buffer)")
    // logDateStats("erp_validated_mis_no write (flussi tecnici)", dfFlussiTecniciNormalized, erpValidatedMisNoSchema.data_misura)
    flussiTecnici.unpersist(blocking = false)

    //flussi Tecnici Misura Smontaggio
    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Smontaggio")
    val dfFlussiTecniciMisureSmis = CalcoloPrelevatoPuntiPrelievoNonOrari
      .flussiMisureSmisSegmentoSinistraEDestro(flussoMisureSmis, year, month, podExcluded)
      .persist()

    val dfFlussiTecniciMisureSmontaggio = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiTecniciMisuraSmontaggio(
        dfFlussiTecniciMisureSmis
        , trattPodllAnnomesePartitionedSegmentoSinistraEDestraBr
        , start_in
        , stop_in
        , timestamp
      )
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Smontaggio")

    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Smontaggio (buffer)")
    val dfFlussiTecniciMisureSmontaggioNormalized = normalizeDataMisura(dfFlussiTecniciMisureSmontaggio)
    validatedParts += dfFlussiTecniciMisureSmontaggioNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Smontaggio (buffer)")
    // logDateStats("erp_validated_mis_no write (SMIS smontaggio)", dfFlussiTecniciMisureSmontaggioNormalized, erpValidatedMisNoSchema.data_misura)

    //flussi Tecnici Misura Montaggio

    logger.info("Inizio Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Montaggio")
    val dfFlussiTecniciMisureMontaggio = CalcoloPrelevatoPuntiPrelievoMisNOTrasformation
      .calcoloPrelevatoPuntiPrelievoNOIngestionFlussiTecniciMisuraMontaggio(
        dfFlussiTecniciMisureSmis
        , trattPodllAnnomesePartitionedSegmentoSinistraEDestraBr
        , start_in
        , stop_in
        , timestamp
      )

    dfFlussiTecniciMisureSmis.unpersist(blocking = false)
    flussoMisureNoAggrSegmentoSinistraEDestra.unpersist(blocking = false)
    flussoMisureSmis.unpersist(blocking = false)
    trattPodllAnnomesePartitionedSegmentoSinistraEDestraJoin.unpersist(blocking = false)
    trattPodllAnnomesePartitionedSegmentoSinistraEDestra.unpersist(blocking = false)
    logger.info("Fine Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Montaggio")

    logger.info(s"Inizio Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Montaggio (buffer)")
    val dfFlussiTecniciMisureMontaggioNormalized = normalizeDataMisura(dfFlussiTecniciMisureMontaggio)
    validatedParts += dfFlussiTecniciMisureMontaggioNormalized
    logger.info(s"Fine Ingestion flussi di misura e validazione misure Flussi Tecnici Misure Montaggio (buffer)")
    // logDateStats("erp_validated_mis_no write (SMIS montaggio)", dfFlussiTecniciMisureMontaggioNormalized, erpValidatedMisNoSchema.data_misura)

    // Consolidated write with dedup per execution
    logger.info("Consolidamento erp_validated_mis_no: union + dedup per execution")
    if (validatedParts.nonEmpty) {
      val unionDf = validatedParts.reduce(_.unionByName(_))
      val dedupDf = dedupValidatedMisNo(unionDf)
      logger.info(s"Scrittura consolidated validated NO su tabella ${erpValidatedMisNODao.tableName}")
      erpValidatedMisNODao.write(dedupDf, overwrite = false)
      // logDateStats("erp_validated_mis_no write (consolidated dedup)", dedupDf, erpValidatedMisNoSchema.data_misura)
    } else {
      logger.warn("Nessun dataset validated NO da scrivere (buffer vuoto)")
    }

  }


}
