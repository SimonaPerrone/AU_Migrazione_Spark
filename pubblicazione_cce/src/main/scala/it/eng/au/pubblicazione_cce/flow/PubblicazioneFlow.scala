package it.eng.au.pubblicazione_cce.flow

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.dao.cce.{CceCalcoloDao, CceEsitoDao, CceEsitoExportDao, CceRichiestaFiltroDao}
import it.eng.au.pubblicazione_cce.flow.ammissibilita.{PubblicazioneAmmissibilitaFileFlow, PubblicazioneAmmissibilitaPodFlow}
import it.eng.au.pubblicazione_cce.flow.consumi.PubblicazioneConsumiFlow
import it.eng.au.pubblicazione_cce.model.cce.{CceEsitoModel, CceEsitoViewModel}
import it.eng.au.pubblicazione_cce.model.flow.EsitoConsumiModel
import it.eng.au.pubblicazione_cce.schema.cce.{CceEsitoSchema, CceEsitoViewSchema, CceRichiestaPodSchema}
import it.eng.au.pubblicazione_cce.schema.flow.EsitoConsumiSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession, functions}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.time.LocalDate
import java.time.format.DateTimeFormatter

class PubblicazioneFlow(val processo: String, val dataRichieste: LocalDate, val misureDao: CceCalcoloDao) extends Serializable {
  @transient private val logger = Logger.getLogger(getClass.getName)

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  val richiestaFiltroDao: CceRichiestaFiltroDao = new CceRichiestaFiltroDao
  val esitoDao: CceEsitoDao = new CceEsitoDao
  val esitoExportDao:CceEsitoExportDao = new CceEsitoExportDao

  val pubblicazioneAmmissibilitaFileFlow: PubblicazioneAmmissibilitaFileFlow =
    new PubblicazioneAmmissibilitaFileFlow(dataRichieste = dataRichieste, processo = processo)

  val pubblicazioneAmmissibilitaPodFlow: PubblicazioneAmmissibilitaPodFlow =
    new PubblicazioneAmmissibilitaPodFlow(dataRichieste = dataRichieste, processo = processo)

  val pubblicazioneConsumiFlow: PubblicazioneConsumiFlow =
    new PubblicazioneConsumiFlow(dataRichieste = dataRichieste, processo = processo, misureDao = misureDao)

  val dataCalcolo: LocalDate = Environment.processDate
  val processTimestamp: Timestamp = Environment.processTimestamp
  val executionId: String = Environment.executionId.toString
  val outputFilePath: String = Environment.getOutputFilePath

  def run(): Unit = {
    logger.warn(s"Inizio processo: $processo, per data richieste: $dataRichieste")

    // Scrittura file ammissibilita' per richieste POD-File
    val esitoAmmissibilitaPodFile = pubblicazioneAmmissibilitaFileFlow.run()
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    // Scrittura file ammissibilita' per richieste POD-POD
    val esitoAmmissibilitaPodPod = pubblicazioneAmmissibilitaPodFlow.run()
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val esitoAmmissibilitaPod = esitoAmmissibilitaPodFile.union(esitoAmmissibilitaPodPod)

    // Scrittura file ZIP consumi ed elenco flussi
    val esitoMisure = pubblicazioneConsumiFlow.run()
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    // Se null allora nessuna richiesta trovata
    if (esitoMisure != null) {
      // Calcolo esito
      // Richieste FILTRO non hanno file prodotti quindi creiamo solo id richiesta e path
      val esitoRichiesteFiltro = calcolaEsitoFiltro(dataRichieste = dataRichieste)

      // Unisci gli esiti dei vari flussi per creare l'esito finale
      val esitoFlussi = calcolaEsitoFlussi(esitoAmmissibilitaPod, esitoRichiesteFiltro, esitoMisure)

      esitoMisure.unpersist()
      logger.warn(s"Scrittura tabella esiti: ${esitoDao.tableName}")

      esitoDao.write(data = esitoFlussi, overwrite = true)
      logger.warn(s"Scrittura tabella esiti export per oracle: ${esitoDao.tableName}")

      val esitoFlussiExport = esitoFlussi.select(
        CceEsitoViewSchema.n_id_richiesta,
        CceEsitoViewSchema.t_path,
        CceEsitoViewSchema.t_file_esito
        ,CceEsitoViewSchema.t_file_ammissibilita,
        CceEsitoViewSchema.t_stato,
        CceEsitoViewSchema.d_data_esito
      ).as[CceEsitoViewModel]

      esitoExportDao.write(data = esitoFlussiExport, overwrite = true)
    }
    logger.warn(s"Fine processo: $processo, per data richieste: $dataRichieste")
  }

  // Calcola esito per richieste FILTRO (per cui non esistono pubblicazioni ammissibilita')
  def calcolaEsitoFiltro(dataRichieste: LocalDate): Dataset[CceEsitoModel] = {
    val annoCalcolo: String = dataCalcolo.getYear.toString
    val meseCalcolo: String = dataCalcolo.format(DateTimeFormatter.ofPattern("MM"))

    richiestaFiltroDao.read()
      .where(col(CceRichiestaPodSchema.partition_request_date) === dataRichieste.toString)
      .where(col(CceRichiestaPodSchema.t_processo) === processo)
      .map(r => CceEsitoModel(
        n_id_richiesta = r.n_id_richiesta,
        t_path = {
          val tipo = if (r.t_ruolo == CostantiCCE.RUOLO_UDD) CostantiCCE.PATH_UDD else CostantiCCE.PATH_ID
          s"${outputFilePath}CCE/$tipo/${r.t_piva}/${r.t_processo}/$annoCalcolo/$meseCalcolo/"
        },
        t_file_esito = null,
        t_file_ammissibilita = null,
        t_stato = null,
        t_operation_name = r.t_processo,
        t_number_file_zip = null,
        execution_id_input_read = null,
        d_data_esito = processTimestamp,
        tipo_richiesta = CostantiCCE.RICHIESTA_FILTRO,
        n_executionid = executionId,
        d_data_richiesta = dataRichieste.toString
      ))
  }

  // calcola tabella esiti finale
  def calcolaEsitoFlussi(
                          esitoAmmissibilitaPod: Dataset[CceEsitoModel],
                          esitoAmmissibilitaFiltro: Dataset[CceEsitoModel],
                          esitoConsumi: Dataset[EsitoConsumiModel]
                        ): Dataset[CceEsitoModel] = {
    // esiti provenienti da calcolo ammissibilita richieste POD e richieste FILTRO calcolate precedentemente
    // queste operazioni valorizzano il path dove sono salvati i file, la lista dei csv prodotti (se creati) e
    // lo stato solo se e' inammissibile (derivante da richiesta pod con file non ammissibile)
    val esitiRichieste = esitoAmmissibilitaPod
      .union(esitoAmmissibilitaFiltro)
      .groupBy(
        CceEsitoSchema.n_id_richiesta,
        CceEsitoSchema.t_path,
        CceEsitoSchema.tipo_richiesta
      )
      .agg(
        sort_array(collect_list(col(CceEsitoSchema.t_file_ammissibilita))).as(CceEsitoSchema.t_file_ammissibilita),
        functions.max(col(CceEsitoSchema.t_stato)).as(CceEsitoSchema.t_stato)
      )

    // esiti richieste join con esito consumi: vengono aggiunte le informazioni sui file prodotti dai consumi e aggiornato
    // lo stato: se presenti misure -> E; se non sono presenti misure per la richiesta -> NC
    esitiRichieste
      .join(esitoConsumi,
        esitiRichieste(CceEsitoSchema.n_id_richiesta) === esitoConsumi(EsitoConsumiSchema.richiesta),
        "LEFT"
      )
      .withColumn(CceEsitoSchema.t_stato,
        when(col(CceEsitoSchema.t_stato).isNotNull, col(CceEsitoSchema.t_stato))
          .otherwise(when(esitoConsumi(EsitoConsumiSchema.richiesta).isNull, lit(CostantiCCE.STATO_NO_CONSUMI))
            .otherwise(lit(CostantiCCE.STATO_ELABORATO)))
      )
      .map(r => {
        val fileEsito = if (r.getAs[List[String]](EsitoConsumiSchema.zipFiles) == null)
          null else r.getAs[List[String]](EsitoConsumiSchema.zipFiles).mkString(";")

        val fileAmmissibilita = if (r.getAs[List[String]](CceEsitoSchema.t_file_ammissibilita) == null ||
          r.getAs[List[String]](CceEsitoSchema.t_file_ammissibilita).isEmpty) {
          null
        } else {
          r.getAs[List[String]](CceEsitoSchema.t_file_ammissibilita).mkString(";")
        }

        CceEsitoModel(
          n_id_richiesta = r.getAs[String](CceEsitoSchema.n_id_richiesta),
          t_path = r.getAs[String](CceEsitoSchema.t_path),
          t_file_esito = fileEsito,
          t_file_ammissibilita = fileAmmissibilita,
          t_stato = r.getAs[String](CceEsitoSchema.t_stato),
          t_operation_name = processo,
          t_number_file_zip = r.getAs[Int](EsitoConsumiSchema.nZipFiles),
          execution_id_input_read = r.getAs[String](EsitoConsumiSchema.execution_id_input_read),
          d_data_esito = processTimestamp,
          tipo_richiesta = r.getAs[String](CceEsitoSchema.tipo_richiesta),
          n_executionid = executionId,
          d_data_richiesta = dataRichieste.toString
        )
      })
  }

}
