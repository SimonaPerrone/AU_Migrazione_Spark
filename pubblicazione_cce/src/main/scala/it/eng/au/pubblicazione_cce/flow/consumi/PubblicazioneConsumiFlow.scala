package it.eng.au.pubblicazione_cce.flow.consumi

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.dao.cce._
import it.eng.au.pubblicazione_cce.file.csv.{ConsumiCsvBuilder, DataFrameCsvBuilder, ElencoFlussiCsvBuilder}
import it.eng.au.pubblicazione_cce.file.delete.FileDelete
import it.eng.au.pubblicazione_cce.file.writer.{FileWriter, ZipWriter}
import it.eng.au.pubblicazione_cce.model.cce._
import it.eng.au.pubblicazione_cce.model.file.{FileConsumiModel, FileElencoFlussiModel}
import it.eng.au.pubblicazione_cce.model.flow.{CalcTrackAggModel, EsitoConsumiModel, PodPubblicazioneModel}
import it.eng.au.pubblicazione_cce.schema.cce._
import it.eng.au.pubblicazione_cce.schema.file.{FileConsumiSchema, FileSchema}
import it.eng.au.pubblicazione_cce.schema.flow.{EsitoConsumiSchema, PodPubblicazioneSchema}
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{date_format, substring, _}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.LocalDate

class PubblicazioneConsumiFlow(dataRichieste: LocalDate, processo: String, val misureDao: CceCalcoloDao) extends Serializable {
  @transient private val logger = Logger.getLogger(getClass.getName)

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  // input
  val richiestePodDao: CceRichiestaPodDao = new CceRichiestaPodDao()
  val richiesteFiltroDao: CceRichiestaFiltroDao = new CceRichiestaFiltroDao()

  val anagraficaPodDao: CceCalcoloAnagraficaDao = new CceCalcoloAnagraficaDao()
  val trattamentoDao: CceCalcoloTrattamentoDao = new CceCalcoloTrattamentoDao()
  val trackDao: CceCalcTrackDao = new CceCalcTrackDao()

  // output
  val outputFileCsvWriter = new FileWriter()
  val outputFileZipWriter = new ZipWriter()

  //output delete tmp
  val outputFileCsvDelete = new FileDelete()



  // env
  val processTimestamp: Timestamp = Environment.processTimestamp
  val fileTimestamp: String = Environment.fileTimestamp
  val consumiCsvBuilder: DataFrameCsvBuilder = new ConsumiCsvBuilder
  val elencoFileCsvBuilder: DataFrameCsvBuilder = new ElencoFlussiCsvBuilder


  def run(): Dataset[EsitoConsumiModel] = {
    // LETTURA DATI INPUT
    logger.warn(s"Inizio Pubblicazione file Consumi: $processo, per data: $dataRichieste")

    logger.warn(s"Lettura anagrafica: ${anagraficaPodDao.tableName}")
    val anagrafica = anagraficaPodDao.read()
      .repartition(col(CceCalcoloAnagraficaSchema.t_codice_pod))

    logger.warn(s"Lettura calcTrack: ${trackDao.tableName}")
    // massima data di calcolo (per processo) per ogni anno mese (con esito OK)
    val track = if (processo == "PR" || processo == "PREIN") {
      trackDao.read()
        .where(upper(col(CceCalcTrackSchema.t_tipo_calc)) === processo)
        .where(col(CceCalcTrackSchema.t_esito) === CostantiCCE.ESITO_OK)
        .where(col(CceCalcTrackSchema.t_mese_calc).isNull)
        .groupBy(CceCalcTrackSchema.t_anno_calc)
        .agg(max(CceCalcTrackSchema.d_data_calc).as(CceCalcTrackSchema.d_data_calc),
          max(CceCalcTrackSchema.executionid).as(CceCalcTrackSchema.executionid))
        .withColumn(CceCalcTrackSchema.t_mese_calc,lit(null))
        .as[CalcTrackAggModel]
        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    }
    else {trackDao.read()
      .where(upper(col(CceCalcTrackSchema.t_tipo_calc)) === processo)
      .where(col(CceCalcTrackSchema.t_esito) === CostantiCCE.ESITO_OK)
      .groupBy(CceCalcTrackSchema.t_anno_calc, CceCalcTrackSchema.t_mese_calc)
      .agg(
        max(CceCalcTrackSchema.d_data_calc).as(CceCalcTrackSchema.d_data_calc),
        max(CceCalcTrackSchema.executionid).as(CceCalcTrackSchema.executionid)
      )
      .as[CalcTrackAggModel]
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)}

    logger.warn(s"Lettura misure: ${misureDao.tableName}")
    val misure_non_filtrate = misureDao.read()
    val  window_spec = Window.partitionBy(CceCalcoloMisureSchema.pod, CceCalcoloMisureSchema.data_misura).orderBy(col(CceCalcoloMisureSchema.executionid).desc)

    val misureTmp = if(processo=="PR"||processo== "PREIN") {
      misure_non_filtrate
        .withColumn("row_number", row_number().over(window_spec))
        .where(col("row_number") === 1)
        .drop("row_number")
        .as[CceCalcoloMisureModel]
    } else {misure_non_filtrate}

    // trattamenti validi
    logger.warn(s"Lettura trattamento: ${trattamentoDao.tableName}")
    val trattamentoTmp = trattamentoDao.read()

    logger.warn(s"Lettura richieste filtro: ${richiesteFiltroDao.tableName}")
    val filtro = richiesteFiltroDao.read()
      .where(col(CceRichiestaFiltroSchema.partition_request_date) === dataRichieste.toString)
      .where(col(CceRichiestaFiltroSchema.t_processo) === processo)

    logger.warn(s"Lettura richieste pod: ${richiestePodDao.tableName}")
    val pod = richiestePodDao.read()
      .where(col(CceRichiestaPodSchema.partition_request_date) === dataRichieste.toString)
      .where(col(CceRichiestaPodSchema.t_processo) === processo)
      .where(col(CceRichiestaPodSchema.t_tipo_amm) === CostantiCCE.RICHIESTA_POD)

    // calcola pod da richieste filtro
    val podRichiesteFiltro = if (filtro.isEmpty) {
      logger.warn(s"Nessuna richiesta FILTRO trovata")
      List.empty[PodPubblicazioneModel].toDS()
    } else {
      calcolaPodRichiesteFiltro(filtro = filtro, anagrafica = anagrafica, track = track)
    }

    // calcola pod da richieste filtro
    val podRichiestePod = if (pod.isEmpty) {
      logger.warn(s"Nessuna richiesta POD trovata")
      List.empty[PodPubblicazioneModel].toDS()
    } else {
      calcolaPodRichiestePod(pod = pod, anagrafica = anagrafica, track = track)
    }

    track.unpersist()


    val podRichieste = podRichiesteFiltro.union(podRichiestePod)
      //controllo a finestra mobile per restringere l'anagrafica
      //filter rows - only where year -month last/first date are between d_inizio_udd and d_fine_udd
      .withColumn("first_day", to_date(concat_ws("-", col(PodPubblicazioneSchema.anno), col(PodPubblicazioneSchema.mese), lit("01")), "yyyy-MM-dd"))
      .withColumn("last_day", last_day($"first_day"))
      .where($"first_day" <= col(CceCalcoloAnagraficaSchema.d_fine_udd) && $"last_day" >= col(CceCalcoloAnagraficaSchema.d_inizio_udd))
      .repartition(col(PodPubblicazioneSchema.cod_pod),concat(col(PodPubblicazioneSchema.anno),col(PodPubblicazioneSchema.mese)))
      .drop("first_day","last_day")
      .as[PodPubblicazioneModel]
    podRichieste.persist()

    if (podRichieste.isEmpty){
      logger.warn("Nessuna richiesta trovata. Termina processo")
//      return null
    }

    // try to filter
    val podRichiesteAnnoSets = podRichieste.select(col(PodPubblicazioneSchema.anno)).distinct().as[String].collect().toSet

    val podRichiesteMeseSets = podRichieste.select(col(PodPubblicazioneSchema.mese)).distinct().as[String].collect().toSet

    val podRichiesteAnnaMeseSets = podRichieste.select(concat(col(PodPubblicazioneSchema.anno),col(PodPubblicazioneSchema.mese))).distinct().as[String].collect().toSet

    val podRichiesteCodPodSets = podRichieste.select(col(PodPubblicazioneSchema.cod_pod)).distinct().as[String].collect().toSet

    val misure = misureTmp
          .filter(col(CceCalcoloMisureSchema.anno).isin(podRichiesteAnnoSets.toSeq: _*))
          .filter(col(CceCalcoloMisureSchema.mese).isin(podRichiesteMeseSets.toSeq: _*))
          .repartition(col(CceCalcoloMisureSchema.pod),concat(col(CceCalcoloMisureSchema.anno),col(CceCalcoloMisureSchema.mese)))

    val trattamento = trattamentoTmp
      .filter(col(CceCalcoloTrattamentoSchema.t_anno_mese).isin(podRichiesteAnnaMeseSets.toSeq: _*))
      .filter(col(CceCalcoloTrattamentoSchema.t_codice_pod).isin(podRichiesteCodPodSets.toSeq: _*))
      .repartition(col(CceCalcoloTrattamentoSchema.t_codice_pod),col(CceCalcoloTrattamentoSchema.t_anno_mese))

    // calcola consumi da pod richieste
    val misurePod = calcolaMisurePod(podRichieste = podRichieste, misure = misure, trattamento = trattamento)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    podRichieste.unpersist()

    // computazione elenco file
    val elencoFileDs = calcolaElencoFile(misurePod)

    val elencoFileCsv = elencoFileCsvBuilder.computeCsvElements(elencoFileDs.toDF)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val elencoFileCsvOutput = elencoFileCsvBuilder.dfToFileModel(elencoFileCsv)
    logger.warn(s"Scrittura file elenco file/flussi")
    outputFileCsvWriter.write(elencoFileCsvOutput)

    val misureCsv = consumiCsvBuilder.computeCsvElements(misurePod.toDF)
      .repartition(col(FileConsumiSchema.id_richiesta))
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    misurePod.unpersist()

    // conversione a dataset (FileModel) per la scrittura del file
    val misureCsvOutput = consumiCsvBuilder.dfToFileModel(misureCsv)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    logger.warn(s"Scrittura file consumi csv")
    outputFileCsvWriter.write(misureCsvOutput)

    misureCsvOutput.unpersist()

    // id richiesta, executionid misure
    val esitoConsumi = calcolaEsito(misureCsv)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val fileCsvElencoFlussi = elencoFileCsv.select(
      FileConsumiSchema.id_richiesta,
      FileConsumiSchema.piva,
      FileConsumiSchema.sessione,
      FileConsumiSchema.processo,
      FileSchema.filePathSubDirectories,
      FileSchema.fileFullName
    )

    val fileCsvMisure = misureCsv.select(
      FileConsumiSchema.id_richiesta,
      FileConsumiSchema.piva,
      FileConsumiSchema.sessione,
      FileConsumiSchema.processo,
      FileSchema.filePathSubDirectories,
      FileSchema.fileFullName
    )


    logger.warn(s"Scrittura file zip")
    val fileCsvProdotti = fileCsvElencoFlussi.union(fileCsvMisure)
    val zipFiles = outputFileZipWriter.computeZipFiles(fileCsvProdotti)

    val esitoZip = outputFileZipWriter.write(zipFiles)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    // da fare attenzione in quanto se ricalcolato riscrive i file zip

    //forse write op to be executed here before deliting csv file later
    val esitoZipStringDump = esitoZip.count()

    elencoFileCsv.unpersist()
    misureCsv.unpersist()

    // aggiungi execution id read (da misure) a esito zip
    val esito =
    esitoConsumi
      .join(broadcast(esitoZip.drop(EsitoConsumiSchema.execution_id_input_read)),
        esitoZip(EsitoConsumiSchema.richiesta) === esitoConsumi(FileConsumiSchema.id_richiesta),
        "INNER"
      )
      .selectExpr(EsitoConsumiSchema.getValues: _*)
      .as[EsitoConsumiModel]

    esitoConsumi.unpersist()

     //delete temporary file
    logger.warn(s"Rimozioni file tmp elenco file/flussi")
    outputFileCsvDelete.delete(elencoFileCsvOutput)

    logger.warn(s"Rimozione file tmp consumi csv")
    outputFileCsvDelete.delete(misureCsvOutput)

    //end delete temporary filesN

    logger.warn(s"Fine Pubblicazione file Consumi: $processo")

    esito
  }

  def calcolaEsito(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy(col(FileConsumiSchema.id_richiesta))
      .agg(collect_set(col(FileConsumiSchema.executionid)).as(FileConsumiSchema.executionid))
      .withColumn(EsitoConsumiSchema.execution_id_input_read, concat_ws(",", col(FileConsumiSchema.executionid)))
  }

  // Calcola DataFrame utile per la scrittura del file ElencoFile
  def calcolaElencoFile(ds: Dataset[FileConsumiModel]): Dataset[FileElencoFlussiModel] = {
    ds.map(r => FileElencoFlussiModel(
      piva = r.piva,
      ruolo = r.ruolo,
      sessione = r.sessione,
      processo = r.processo,
      annomese = r.anno + r.mese,
      timestamp = fileTimestamp,
      id_richiesta = r.id_richiesta,
      pod = r.cod_pod,
      path_cloud = r.nome_file,
      data_aggiornamento = r.data_aggiornamento
    )).dropDuplicates()
  }

  // Ritorna pod da pubblicare per le richieste POD
  def calcolaPodRichiestePod(pod: Dataset[CceRichiestaPodModel],
                             anagrafica: Dataset[CceCalcoloAnagraficaModel],
                             track: Dataset[CalcTrackAggModel]
                            ): Dataset[PodPubblicazioneModel] = {
    val colMantieniPerRuolo = "_mantieni_ruolo"

    pod
      // per ogni anno e mese della richiesta prendi executionid da usare per leggere misure
      .join(broadcast(track),
        pod(CceRichiestaPodSchema.t_anno) === track(CceCalcTrackSchema.t_anno_calc),
        "LEFT"
      )
      // se in richiesta mese vuoto allora prendi tutti i mesi, altrimenti mese richiesta
      .where(pod(CceRichiestaPodSchema.t_mese).isNull or
        (pod(CceRichiestaPodSchema.t_mese) === lpad(track(CceCalcTrackSchema.t_mese_calc),2,"0")
          or lit(processo) ===lit("PR")  or lit(processo) ===lit("PREIN")))
      // dati anagrafica pod
      .join(anagrafica,
        pod(CceRichiestaPodSchema.t_codice_pod) === anagrafica(CceCalcoloAnagraficaSchema.t_codice_pod),
        "INNER"
      )
      // verifica coerenza per RUOLO richiesta
      .withColumn(colMantieniPerRuolo, when(pod(CceRichiestaPodSchema.t_ruolo) === CostantiCCE.RUOLO_DISTR,
        pod(CceRichiestaPodSchema.t_piva) === anagrafica(CceCalcoloAnagraficaSchema.t_piva_id))
        .otherwise(when(pod(CceRichiestaPodSchema.t_ruolo) === CostantiCCE.RUOLO_UDD,
          pod(CceRichiestaPodSchema.t_piva) === anagrafica(CceCalcoloAnagraficaSchema.t_piva_udd))
          .otherwise(lit(true))) // CostantiCCE.RUOLO_SII
      )
      .where(col(colMantieniPerRuolo) === true)
      // seleziona colonne finali
      .select(
        pod(CceRichiestaPodSchema.t_piva) as PodPubblicazioneSchema.piva,
        pod(CceRichiestaPodSchema.t_ruolo) as PodPubblicazioneSchema.ruolo,
        pod(CceRichiestaPodSchema.t_servizio) as PodPubblicazioneSchema.sessione,
        pod(CceRichiestaPodSchema.t_processo) as PodPubblicazioneSchema.processo,
        pod(CceRichiestaPodSchema.t_anno) as PodPubblicazioneSchema.anno,
        pod(CceRichiestaPodSchema.t_mese) as PodPubblicazioneSchema.mese,
        pod(CceRichiestaPodSchema.n_id_richiesta) as PodPubblicazioneSchema.id_richiesta,
        pod(CceRichiestaPodSchema.t_codice_pod) as PodPubblicazioneSchema.cod_pod,
        anagrafica(CceCalcoloAnagraficaSchema.t_piva_id) as PodPubblicazioneSchema.piva_distr,
        anagrafica(CceCalcoloAnagraficaSchema.t_piva_udd) as PodPubblicazioneSchema.piva_udd,
        anagrafica(CceCalcoloAnagraficaSchema.d_inizio_udd) as PodPubblicazioneSchema.d_inizio_udd,
        anagrafica(CceCalcoloAnagraficaSchema.d_fine_udd) as PodPubblicazioneSchema.d_fine_udd,
        track(CceCalcTrackSchema.d_data_calc) as PodPubblicazioneSchema.d_data_calc,
        track(CceCalcTrackSchema.executionid) as PodPubblicazioneSchema.executionid
      )
      .as[PodPubblicazioneModel]
  }

  // Ritorna pod per le richieste Filtro
  def calcolaPodRichiesteFiltro(filtro: Dataset[CceRichiestaFiltroModel],
                                anagrafica: Dataset[CceCalcoloAnagraficaModel],
                                track: Dataset[CalcTrackAggModel]
                               ): Dataset[PodPubblicazioneModel] = {
    val colMantieniPerRuolo = "_mantieni_ruolo"

    anagrafica
      .crossJoin(broadcast(filtro))
      // verifiche filtri richiesta per filtrare POD da pubblicare
      .where(filtro(CceRichiestaFiltroSchema.t_tensione) === anagrafica(CceCalcoloAnagraficaSchema.t_tensione))
      .where(filtro(CceRichiestaFiltroSchema.t_zona) === anagrafica(CceCalcoloAnagraficaSchema.t_area_rif))
      // opzionali: se valorizzato in richiesta filtra altrimenti ignora
      .where(filtro(CceRichiestaFiltroSchema.t_tipo_pod).isNull or
        filtro(CceRichiestaFiltroSchema.t_tipo_pod) === anagrafica(CceCalcoloAnagraficaSchema.t_tipo_pod))
      .where(filtro(CceRichiestaFiltroSchema.t_tariffa).isNull or
        filtro(CceRichiestaFiltroSchema.t_tariffa) === anagrafica(CceCalcoloAnagraficaSchema.t_tariffa_distr))
      .where(filtro(CceRichiestaFiltroSchema.t_codice_terna).isNull or
        filtro(CceRichiestaFiltroSchema.t_codice_terna) === anagrafica(CceCalcoloAnagraficaSchema.t_codice_terna))
      .where(filtro(CceRichiestaFiltroSchema.t_piva_id).isNull or
        (filtro(CceRichiestaFiltroSchema.t_piva_id) === anagrafica(CceCalcoloAnagraficaSchema.t_piva_id)))
      .where(filtro(CceRichiestaFiltroSchema.t_piva_udd).isNull or
        (filtro(CceRichiestaFiltroSchema.t_piva_udd) === anagrafica(CceCalcoloAnagraficaSchema.t_piva_udd)))
      // verifica coerenza per RUOLO richiesta
      .withColumn(colMantieniPerRuolo, when(filtro(CceRichiestaFiltroSchema.t_ruolo) === CostantiCCE.RUOLO_DISTR,
        filtro(CceRichiestaFiltroSchema.t_piva) === anagrafica(CceCalcoloAnagraficaSchema.t_piva_id))
        .otherwise(when(filtro(CceRichiestaFiltroSchema.t_ruolo) === CostantiCCE.RUOLO_UDD,
          filtro(CceRichiestaFiltroSchema.t_piva) === anagrafica(CceCalcoloAnagraficaSchema.t_piva_udd))
          .otherwise(lit(true))) // CostantiCCE.RUOLO_SII
      )
      .where(col(colMantieniPerRuolo) === true)
      // per ogni anno e mese della richiesta prendi executionid da usare per leggere misure
      .join(broadcast(track),
        filtro(CceRichiestaFiltroSchema.t_anno) === track(CceCalcTrackSchema.t_anno_calc),
        "LEFT"
      )
      // se in richiesta mese vuoto allora prendi tutti i mesi, altrimenti mese richiesta
      .where(filtro(CceRichiestaFiltroSchema.t_mese).isNull or
        (filtro(CceRichiestaFiltroSchema.t_mese) === lpad(track(CceCalcTrackSchema.t_mese_calc),2,"0")
          or lit(processo) ===lit("PR")  or lit(processo) ===lit("PREIN")))
      // seleziona colonne finali
      .select(
        filtro(CceRichiestaFiltroSchema.t_piva) as PodPubblicazioneSchema.piva,
        filtro(CceRichiestaFiltroSchema.t_ruolo) as PodPubblicazioneSchema.ruolo,
        filtro(CceRichiestaFiltroSchema.t_servizio) as PodPubblicazioneSchema.sessione,
        filtro(CceRichiestaFiltroSchema.t_processo) as PodPubblicazioneSchema.processo,
        filtro(CceRichiestaFiltroSchema.t_anno) as PodPubblicazioneSchema.anno,
        filtro(CceRichiestaFiltroSchema.t_mese) as PodPubblicazioneSchema.mese,
        filtro(CceRichiestaFiltroSchema.n_id_richiesta) as PodPubblicazioneSchema.id_richiesta,
        anagrafica(CceCalcoloAnagraficaSchema.t_codice_pod) as PodPubblicazioneSchema.cod_pod,
        anagrafica(CceCalcoloAnagraficaSchema.t_piva_id) as PodPubblicazioneSchema.piva_distr,
        anagrafica(CceCalcoloAnagraficaSchema.t_piva_udd) as PodPubblicazioneSchema.piva_udd,
        anagrafica(CceCalcoloAnagraficaSchema.d_inizio_udd) as PodPubblicazioneSchema.d_inizio_udd,
        anagrafica(CceCalcoloAnagraficaSchema.d_fine_udd) as PodPubblicazioneSchema.d_fine_udd,
        track(CceCalcTrackSchema.d_data_calc) as PodPubblicazioneSchema.d_data_calc,
        track(CceCalcTrackSchema.executionid) as PodPubblicazioneSchema.executionid
      )
      //.dropDuplicates() // può una richiesta filtro selezionare piu volte lo stesso pod?
      .as[PodPubblicazioneModel]
  }

  def calcolaMisurePod(podRichieste: Dataset[PodPubblicazioneModel],
                       misure: Dataset[CceCalcoloMisureModel],
                       trattamento: Dataset[CceCalcoloTrattamentoModel]
                      ): Dataset[FileConsumiModel] = {
    podRichieste
      .join(trattamento,
        podRichieste(PodPubblicazioneSchema.cod_pod) === trattamento(CceCalcoloTrattamentoSchema.t_codice_pod) and
          concat(podRichieste(PodPubblicazioneSchema.anno), podRichieste(PodPubblicazioneSchema.mese)) === trattamento(CceCalcoloTrattamentoSchema.t_anno_mese),
        "LEFT")
      .where(col(CceCalcoloTrattamentoSchema.is_t_trattamento) === CostantiCCE.TRATTAMENTO_Y)
      .join(
        misure,
        podRichieste(PodPubblicazioneSchema.cod_pod) === misure(CceCalcoloMisureSchema.pod) and
          podRichieste(PodPubblicazioneSchema.anno) === misure(CceCalcoloMisureSchema.anno) and
          podRichieste(PodPubblicazioneSchema.mese) === misure(CceCalcoloMisureSchema.mese)
       , "INNER"
      )
      // verifica coerenza misure con periodo UDD
      .where(misure(CceCalcoloMisureSchema.data_misura).between(
        substring(podRichieste(PodPubblicazioneSchema.d_inizio_udd), 1, 10),
        substring(podRichieste(PodPubblicazioneSchema.d_fine_udd), 1, 10)))
      // seleziona colonne finali
      .select(
        podRichieste(PodPubblicazioneSchema.piva) as FileConsumiSchema.piva,
        podRichieste(PodPubblicazioneSchema.ruolo) as FileConsumiSchema.ruolo,
        podRichieste(PodPubblicazioneSchema.sessione) as FileConsumiSchema.sessione,
        podRichieste(PodPubblicazioneSchema.processo) as FileConsumiSchema.processo,
        podRichieste(PodPubblicazioneSchema.anno) as FileConsumiSchema.anno,
        podRichieste(PodPubblicazioneSchema.mese) as FileConsumiSchema.mese,
        lit(processTimestamp) as FileConsumiSchema.timestamp,
        podRichieste(PodPubblicazioneSchema.id_richiesta) as FileConsumiSchema.id_richiesta,
        misure(CceCalcoloMisureSchema.data_misura) as FileConsumiSchema.data,
        misure(CceCalcoloMisureSchema.pod) as FileConsumiSchema.cod_pod,
        podRichieste(PodPubblicazioneSchema.piva_distr) as FileConsumiSchema.piva_distr,
        podRichieste(PodPubblicazioneSchema.piva_udd) as FileConsumiSchema.piva_udd,
        misure(CceCalcoloMisureSchema.h01) as FileConsumiSchema.h01,
        misure(CceCalcoloMisureSchema.h02) as FileConsumiSchema.h02,
        misure(CceCalcoloMisureSchema.h03) as FileConsumiSchema.h03,
        misure(CceCalcoloMisureSchema.h04) as FileConsumiSchema.h04,
        misure(CceCalcoloMisureSchema.h05) as FileConsumiSchema.h05,
        misure(CceCalcoloMisureSchema.h06) as FileConsumiSchema.h06,
        misure(CceCalcoloMisureSchema.h07) as FileConsumiSchema.h07,
        misure(CceCalcoloMisureSchema.h08) as FileConsumiSchema.h08,
        misure(CceCalcoloMisureSchema.h09) as FileConsumiSchema.h09,
        misure(CceCalcoloMisureSchema.h10) as FileConsumiSchema.h10,
        misure(CceCalcoloMisureSchema.h11) as FileConsumiSchema.h11,
        misure(CceCalcoloMisureSchema.h12) as FileConsumiSchema.h12,
        misure(CceCalcoloMisureSchema.h13) as FileConsumiSchema.h13,
        misure(CceCalcoloMisureSchema.h14) as FileConsumiSchema.h14,
        misure(CceCalcoloMisureSchema.h15) as FileConsumiSchema.h15,
        misure(CceCalcoloMisureSchema.h16) as FileConsumiSchema.h16,
        misure(CceCalcoloMisureSchema.h17) as FileConsumiSchema.h17,
        misure(CceCalcoloMisureSchema.h18) as FileConsumiSchema.h18,
        misure(CceCalcoloMisureSchema.h19) as FileConsumiSchema.h19,
        misure(CceCalcoloMisureSchema.h20) as FileConsumiSchema.h20,
        misure(CceCalcoloMisureSchema.h21) as FileConsumiSchema.h21,
        misure(CceCalcoloMisureSchema.h22) as FileConsumiSchema.h22,
        misure(CceCalcoloMisureSchema.h23) as FileConsumiSchema.h23,
        misure(CceCalcoloMisureSchema.h24) as FileConsumiSchema.h24,
        misure(CceCalcoloMisureSchema.h25) as FileConsumiSchema.h25,
        date_format(podRichieste(PodPubblicazioneSchema.d_data_calc).cast(TimestampType), "yyyyMMddHHmmss") as FileConsumiSchema.data_aggiornamento,
        misure(CceCalcoloMisureSchema.nome_file) as FileConsumiSchema.nome_file,
        podRichieste(CceCalcoloMisureSchema.executionid) as FileConsumiSchema.executionid
      )
      .as[FileConsumiModel]
  }

}
