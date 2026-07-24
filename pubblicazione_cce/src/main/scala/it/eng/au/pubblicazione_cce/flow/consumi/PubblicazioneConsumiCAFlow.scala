package it.eng.au.pubblicazione_cce.flow.consumi

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.dao.cce._
import it.eng.au.pubblicazione_cce.file.csv.{ConsumiCaCsvBuilder, DataFrameCsvBuilder, ElencoFlussiCACsvBuilder}
import it.eng.au.pubblicazione_cce.file.delete.FileDelete
import it.eng.au.pubblicazione_cce.model.file.{FileConsumiCaExplodedModel, FileElencoFlussiCaModel}
import it.eng.au.pubblicazione_cce.model.flow.PodPubblicazioneModel
import it.eng.au.pubblicazione_cce.schema.flow.PodPubblicazioneSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.TimestampType
import it.eng.au.pubblicazione_cce.file.writer.{FileWriter, ZipWriter}
import it.eng.au.pubblicazione_cce.model.cce._
import it.eng.au.pubblicazione_cce.model.file.{FileConsumiCaModel, FileConsumiModel, FileElencoFlussiModel}
import it.eng.au.pubblicazione_cce.model.flow.{CalcTrackAggModel, EsitoConsumiModel}
import it.eng.au.pubblicazione_cce.schema.cce._
import it.eng.au.pubblicazione_cce.schema.file.{FileConsumiCaSchema, FileConsumiSchema, FileSchema}
import it.eng.au.pubblicazione_cce.schema.flow.EsitoConsumiSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.LocalDate

class PubblicazioneConsumiCAFlow(dataRichieste: LocalDate) extends Serializable {
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

  val cceCalcoloCaDao = new CceCalcoloCaDao
  val cceCalcoloCaFlussiDao = new CceCalcoloCaFlussiDao

  // env
  val processTimestamp: Timestamp = Environment.processTimestamp
  val fileTimestamp: String = Environment.fileTimestamp
  val consumiCsvBuilder: DataFrameCsvBuilder = new ConsumiCaCsvBuilder
  val elencoFileCsvBuilder: DataFrameCsvBuilder = new ElencoFlussiCACsvBuilder

  val processo: String = CostantiCCE.PROCESSO_PR
  val processoCA: String = CostantiCCE.PROCESSO_CA
  val misureDao: CceCalcoloPRDao = new CceCalcoloPRDao()

  //output delete tmp
  val outputFileCsvDelete = new FileDelete()

  def run(): Dataset[EsitoConsumiModel] = {
    // LETTURA DATI INPUT
    logger.warn(s"Inizio Pubblicazione file Consumi: $processo, per data: $dataRichieste")

    logger.warn(s"Lettura anagrafica: ${anagraficaPodDao.tableName}")
    val anagrafica = anagraficaPodDao.read()
      .repartition(col(CceCalcoloAnagraficaSchema.t_codice_pod))

    logger.warn(s"Lettura calcTrack: ${trackDao.tableName}")
    // massima data di calcolo (per processo) per ogni anno mese (con esito OK)
    val track =
      trackDao.read()
        .where(upper(col(CceCalcTrackSchema.t_tipo_calc)) === processo)
        .where(col(CceCalcTrackSchema.t_esito) === CostantiCCE.ESITO_OK)
        .where(col(CceCalcTrackSchema.t_mese_calc).isNull)
        .groupBy(CceCalcTrackSchema.t_anno_calc)
        .agg(
          max(CceCalcTrackSchema.d_data_calc).as(CceCalcTrackSchema.d_data_calc),
          max(CceCalcTrackSchema.executionid).as(CceCalcTrackSchema.executionid)
        )
        .withColumn(CceCalcTrackSchema.t_mese_calc,lit(null))
        .as[CalcTrackAggModel]
        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    logger.warn(s"Lettura misure: ${misureDao.tableName}")
    val misure_non_filtrate = misureDao.read()
    val  window_spec = Window.partitionBy(CceCalcoloMisureSchema.pod, CceCalcoloMisureSchema.data_misura).orderBy(col(CceCalcoloMisureSchema.executionid).desc)

    val misureTmp =
      misure_non_filtrate
        .withColumn("row_number", row_number().over(window_spec))
        .where(col("row_number") === 1)
        .drop("row_number")
        .as[CceCalcoloMisureModel]

    // trattamenti validi
    logger.warn(s"Lettura trattamento: ${trattamentoDao.tableName}")
    val trattamentoTmp = trattamentoDao.read()

    logger.warn(s"Lettura richieste filtro: ${richiesteFiltroDao.tableName}")
    val filtro = richiesteFiltroDao.read()
      .where(col(CceRichiestaFiltroSchema.partition_request_date) === dataRichieste.toString)
      .where(col(CceRichiestaFiltroSchema.t_processo) === processoCA)

    logger.warn(s"Lettura richieste pod: ${richiestePodDao.tableName}")
    val pod = richiestePodDao.read()
      .where(col(CceRichiestaPodSchema.partition_request_date) === dataRichieste.toString)
      .where(col(CceRichiestaPodSchema.t_processo) === processoCA)
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
      .withColumn("first_day_of_year", to_date(concat_ws("-", col(PodPubblicazioneSchema.anno), lit("01"), lit("01")), "yyyy-MM-dd"))
      .withColumn("last_day_of_year", to_date(concat_ws("-", col(PodPubblicazioneSchema.anno), lit("12"), lit("31")), "yyyy-MM-dd"))
      .where($"first_day_of_year" <= col(CceCalcoloAnagraficaSchema.d_fine_udd) && $"last_day_of_year" >= col(CceCalcoloAnagraficaSchema.d_inizio_udd))
      .repartition(col(PodPubblicazioneSchema.cod_pod),concat(col(PodPubblicazioneSchema.anno),col(PodPubblicazioneSchema.mese)))
      .drop("first_day_of_year","last_day_of_year")
      .as[PodPubblicazioneModel]
    podRichieste.persist()

    if (podRichieste.isEmpty){
      logger.warn("Non esistono richieste che producono consumi.")
      //return null
    }

    // try to filter
    val podRichiesteAnnoSets = podRichieste.select(col(PodPubblicazioneSchema.anno)).distinct().as[String].collect().toSet

    val podRichiesteCodPodSets = podRichieste.select(col(PodPubblicazioneSchema.cod_pod)).distinct().as[String].collect().toSet

    val misure = misureTmp
            .filter(col(CceCalcoloMisureSchema.anno).isin(podRichiesteAnnoSets.toSeq: _*))
      .filter(col(CceCalcoloMisureSchema.pod).isin(podRichiesteCodPodSets.toSeq: _*))
      .as[CceCalcoloMisureModel]
      .repartition(col(CceCalcoloMisureSchema.pod),concat(col(CceCalcoloMisureSchema.anno),col(CceCalcoloMisureSchema.mese)))

    val trattamento = trattamentoTmp
      .withColumn("anno",substring(col(CceCalcoloTrattamentoSchema.t_anno_mese),1,4))
      .filter(col("anno").isin(podRichiesteAnnoSets.toSeq: _*))
      .filter(col(CceCalcoloTrattamentoSchema.t_codice_pod).isin(podRichiesteCodPodSets.toSeq: _*))
      .drop("anno")
      .as[CceCalcoloTrattamentoModel]
      .repartition(col(CceCalcoloTrattamentoSchema.t_codice_pod))

    // calcola consumi da pod richieste
    val misurePod = calcolaMisurePod(podRichieste = podRichieste, misure = misure, trattamento = trattamento)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    podRichieste.unpersist()

    // calcolo aggregazione CA: fino a questo punto il calcolo e' uguale al calcolo PR
    val misureCa = calcolaCa(misurePod)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val cceCalcoloCaDs = calcolaTabellaCalcoloCa(misureCa, Environment.processTimestamp.toString)

    logger.warn(s"Scrittura tabella: ${cceCalcoloCaDao.tableName}")
    cceCalcoloCaDao.write(cceCalcoloCaDs)

    val cceCalcoloCaFlussiDs = calcolaTabellaCalcoloCaFlussi(misureCa, Environment.processTimestamp.toString)
    logger.warn(s"Scrittura tabella: ${cceCalcoloCaFlussiDao.tableName}")
    cceCalcoloCaFlussiDao.write(cceCalcoloCaFlussiDs)

    // computazione elenco file
    val elencoFileDs = calcolaElencoFile(misureCa)

    val elencoFileCsv = elencoFileCsvBuilder.computeCsvElements(elencoFileDs.toDF)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val elencoFileCsvOutput = elencoFileCsvBuilder.dfToFileModel(elencoFileCsv)
    logger.warn(s"Scrittura file elenco file")
    outputFileCsvWriter.write(elencoFileCsvOutput)

    val misureCsv = consumiCsvBuilder.computeCsvElements(misureCa.toDF)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    misureCa.unpersist()

    // conversione a dataset (FileModel) per la scrittura del file
    val misureCsvOutput = consumiCsvBuilder.dfToFileModel(misureCsv)

    logger.warn(s"Scrittura file consumi csv")
    outputFileCsvWriter.write(misureCsvOutput)

    // id richiesta, executionid misure
    val esitoConsumi = calcolaEsito(misureCsv)
      .persist(StorageLevel.DISK_ONLY_2)

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

    elencoFileCsv.unpersist()
    misureCsv.unpersist()

    logger.warn(s"Scrittura file zip")
    val fileCsvProdotti = fileCsvElencoFlussi.union(fileCsvMisure)
    val zipFiles = outputFileZipWriter.computeZipFiles(fileCsvProdotti)

    val esitoZip = outputFileZipWriter.write(zipFiles)

    // aggiungi execution id read (da misure) a esito zip
    val esito = esitoZip
      .drop(EsitoConsumiSchema.execution_id_input_read) // a NULL, da valorizzare con join successiva
      .join(esitoConsumi,
        esitoZip(EsitoConsumiSchema.richiesta) === esitoConsumi(FileConsumiSchema.id_richiesta),
        "INNER"
      )
      .selectExpr(EsitoConsumiSchema.getValues: _*)
      .as[EsitoConsumiModel]

//force esitoZip to be executed before delete csv files
//    val esitoZipStringDump = esitoZip.count()
    esitoConsumi.unpersist()

//delete temporary file
//    logger.warn(s"Rimozioni file tmp elenco file/flussi")
//    outputFileCsvDelete.delete(elencoFileCsvOutput)
//
//    logger.warn(s"Rimozione file tmp consumi csv")
//    outputFileCsvDelete.delete(misureCsvOutput)

    //end delete temporary files

    logger.warn(s"Fine Pubblicazione file Consumi: $processo")

    esito
  }

  def calcolaEsito(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .dropDuplicates(FileConsumiSchema.id_richiesta, FileConsumiSchema.executionid)
      .select(FileConsumiSchema.id_richiesta, FileConsumiSchema.executionid)
      .withColumnRenamed(FileConsumiSchema.executionid, EsitoConsumiSchema.execution_id_input_read)
  }

  // Calcola DataFrame utile per la scrittura del file ElencoFile
  def calcolaElencoFile(ds: Dataset[FileConsumiCaModel]): Dataset[FileElencoFlussiCaModel] = {
    val ds_exploded = ds.withColumn(FileConsumiCaSchema.nome_file,explode(col(FileConsumiCaSchema.nome_file)))
      .as[FileConsumiCaExplodedModel]

    ds_exploded.map(r =>
      FileElencoFlussiCaModel(
      piva = r.piva,
      ruolo = r.ruolo,
      sessione = r.sessione,
      processo = r.processo,
      anno = r.anno,
      timestamp = fileTimestamp,
      id_richiesta = r.id_richiesta,
      pod = r.cod_pod,
      path_cloud = r.nome_file
    )
    )
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
      filtro(CceRichiestaFiltroSchema.t_anno
      ) === track(CceCalcTrackSchema.t_anno_calc),
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

    def calcolaMisurePod(podRichieste: Dataset[PodPubblicazioneModel],
                         misure: Dataset[CceCalcoloMisureModel],
                         trattamento: Dataset[CceCalcoloTrattamentoModel]
                        ): Dataset[FileConsumiModel] = {
      val trattamentoEnriched = trattamento.
        withColumn("anno",substring(col(CceCalcoloTrattamentoSchema.t_anno_mese),1,4))
        .withColumn("mese",substring(col(CceCalcoloTrattamentoSchema.t_anno_mese),5,2))
      podRichieste
        .join(trattamentoEnriched,
          podRichieste(PodPubblicazioneSchema.cod_pod) === trattamento(CceCalcoloTrattamentoSchema.t_codice_pod) and
            podRichieste(PodPubblicazioneSchema.anno) === trattamentoEnriched("anno"),
          "LEFT")
        .where(col(CceCalcoloTrattamentoSchema.is_t_trattamento) === CostantiCCE.TRATTAMENTO_Y)
        .join(
          misure,
          podRichieste(PodPubblicazioneSchema.cod_pod) === misure(CceCalcoloMisureSchema.pod) and
            podRichieste(PodPubblicazioneSchema.anno) === misure(CceCalcoloMisureSchema.anno) and
            trattamentoEnriched("mese") === misure(CceCalcoloMisureSchema.mese)
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

  def calcolaCa(misurePod: Dataset[FileConsumiModel]): Dataset[FileConsumiCaModel] = {
    // aggregare consumi prima per giorno e poi per anno
    val consumiGiornalieriCol = "_tmp_cg"
    misurePod
      .withColumn(consumiGiornalieriCol,
        col(FileConsumiSchema.h01) + col(FileConsumiSchema.h02) + col(FileConsumiSchema.h03) + col(FileConsumiSchema.h04)
          + col(FileConsumiSchema.h05) + col(FileConsumiSchema.h06) + col(FileConsumiSchema.h07)
          + col(FileConsumiSchema.h08) + col(FileConsumiSchema.h09) + col(FileConsumiSchema.h10)
          + col(FileConsumiSchema.h11) + col(FileConsumiSchema.h12) + col(FileConsumiSchema.h13)
          + col(FileConsumiSchema.h14) + col(FileConsumiSchema.h15) + col(FileConsumiSchema.h16)
          + col(FileConsumiSchema.h17) + col(FileConsumiSchema.h18) + col(FileConsumiSchema.h19)
          + col(FileConsumiSchema.h20) + col(FileConsumiSchema.h21) + col(FileConsumiSchema.h22)
          + col(FileConsumiSchema.h23) + col(FileConsumiSchema.h24) + col(FileConsumiSchema.h25))
      .groupBy(
        FileConsumiCaSchema.id_richiesta,
        FileConsumiCaSchema.anno,
        FileConsumiCaSchema.processo, //da vedere - ridondante? a braccietto con  richiesta/ potrebbe servire come campo dopo
        FileConsumiCaSchema.sessione, //da vedere - rindondante? a braccietto con  richiesta /potrebbe servire come campo dopo
        FileConsumiCaSchema.cod_pod,
        FileConsumiCaSchema.piva, //da vedere - rindondante? a braccietto con  richiesta /potrebbe servire come campo dopo
        FileConsumiCaSchema.piva_distr,
        FileConsumiCaSchema.piva_udd,
        FileConsumiCaSchema.ruolo ,
        FileConsumiCaSchema.timestamp ,
        FileConsumiCaSchema.data_aggiornamento , //non serve
        FileConsumiCaSchema.executionid //della track
      )
      .agg(
        sum(col(consumiGiornalieriCol)).as(FileConsumiCaSchema.ca),
        collect_set(col(FileConsumiSchema.nome_file)).as(FileConsumiCaSchema.nome_file)
      )
      //arrotondarre alla terza cifra dopo la virgola
      .withColumn(FileConsumiCaSchema.ca,round(col(FileConsumiCaSchema.ca),3))
      .selectExpr(FileConsumiCaSchema.getValues: _*)
      .as[FileConsumiCaModel]
  }

  def calcolaTabellaCalcoloCa(consumiCa: Dataset[FileConsumiCaModel], tsElaborazione: String): Dataset[CceCalcoloCaModel] = {
    consumiCa
      .map(r => CceCalcoloCaModel(
        n_id_richiesta = r.id_richiesta,
        anno = r.anno,
        cod_pod = r.cod_pod,
        piva_distr = r.piva_distr,
        piva_udd = r.piva_udd,
        ca = r.ca,
        data_aggiornamento = r.data_aggiornamento,
        d_data_elaborazione = tsElaborazione
      ))
  }

  def calcolaTabellaCalcoloCaFlussi(consumiCa: Dataset[FileConsumiCaModel], tsElaborazione: String): Dataset[CceCalcoloCaFlussiModel] = {
    consumiCa
      .withColumn(CceCalcoloCaFlussiSchema.path_file, explode(col(FileConsumiCaSchema.nome_file)))
      .select(
        col(FileConsumiCaSchema.id_richiesta).as(CceCalcoloCaFlussiSchema.n_id_richiesta),
        col(FileConsumiCaSchema.cod_pod).as(CceCalcoloCaFlussiSchema.pod),
        col(CceCalcoloCaFlussiSchema.path_file),
        lit(tsElaborazione).as(CceCalcoloCaFlussiSchema.d_data_elaborazione)
      )
      .as[CceCalcoloCaFlussiModel]
  }
}
