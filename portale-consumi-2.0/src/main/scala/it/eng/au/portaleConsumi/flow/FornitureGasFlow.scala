package it.eng.au.portaleConsumi.flow

import it.eng.au.portaleConsumi.dao.hive.misuregas.FornitureProcessiGasDao
import it.eng.au.portaleConsumi.dao.hive.rcu.VRcuAziendaPDao
import it.eng.au.portaleConsumi.dao.hive.rcugas._
import it.eng.au.portaleConsumi.dao.hive.switch_gas.PrtSwgPDao
import it.eng.au.portaleConsumi.dao.hive.tdg.TdgVulnPDao
import it.eng.au.portaleConsumi.dao.mongodb.forniture.FornitureGasMongoDbDao
import it.eng.au.portaleConsumi.model.hive.misuregas.FornitureProcessiGasModel
import it.eng.au.portaleConsumi.model.mongodb.forniture._
import it.eng.au.portaleConsumi.schema.misuregas.FornitureProcessiGasSchema
import it.eng.au.portaleConsumi.schema.mongodb.forniture.FornitureGasMongoDbSchema
import it.eng.au.portaleConsumi.schema.rcu.VRcuAziendaPSchema
import it.eng.au.portaleConsumi.schema.rcugas._
import it.eng.au.portaleConsumi.schema.switch_gas.PrtSwgPSchema
import it.eng.au.portaleConsumi.schema.tdg.TdgVulnPSchema
import it.eng.au.portaleConsumi.utility.args.PortaleConsumiArgs
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp
import java.text.SimpleDateFormat

class FornitureGasFlow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Calcola le forniture del gas ed aggiorna il database con le righe aggiornate rispetto l'ultimo calcolo
   *
   */
  def run(flowArgsConfig: PortaleConsumiArgs): Unit = {
    logger.warn("Inizio processo Forniture Gas")
    // da file di proprietà
    val maxDocumentiScritturaDelta = Environment.getProperty("params.mongodb.max_doc_delta_forniture").toLong
    val giorniInizioCalcolo = if (flowArgsConfig.dayInterval == 0) {
      Environment.getProperty("params.job.days_interval_max").toInt
    }
    else {
      flowArgsConfig.dayInterval
    }

    // da parametri
    val dataCalcolo = flowArgsConfig.runDay.toString
    val dataInizioCalcolo = flowArgsConfig.runDay.minusDays(giorniInizioCalcolo)
    val interval = flowArgsConfig.interval
    // variabili processo
    val dataInizioCalcoloTs = Timestamp.valueOf(dataInizioCalcolo + " 00:00:00")
    val dataCalcoloTs = Timestamp.valueOf(dataCalcolo + " 00:00:00")
    val fornitureProcessiGasDao = new FornitureProcessiGasDao()

    logger.warn(s"Numero giorni nel passato: $giorniInizioCalcolo")
    logger.warn(s"Intervallo calcolo: $dataInizioCalcolo - $dataCalcolo")

    val inputDs = leggiDatiInput(dataInizioCalcoloTs, dataCalcoloTs)
    val fornitureCalcolate = calcolaForniture(
      forniture = inputDs("forniture"),
      cliente = inputDs("cliente"),
      residenza = inputDs("residenza"),
      pdr = inputDs("pdr"),
      prelievi = inputDs("prelievi"),
      misuratori = inputDs("misuratori"),
      indirizzi = inputDs("indirizzi"),
      venditori = inputDs("venditori"),
      aziende = inputDs("aziende"),
      connessioni = inputDs("connessioni"),
      distributori = inputDs("distributori"),
      processi = inputDs("processi"),
      offerte = inputDs("offerte"),
      vulnerabilita = inputDs("vulnerabilita"),
      dataCalcolo = dataCalcolo
    ).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    inputDs("forniture").unpersist()
    inputDs("prelievi").unpersist()
    inputDs("misuratori").unpersist()
    inputDs("connessioni").unpersist()
    inputDs("processi").unpersist()

    logger.warn(s"Calcolo forniture")
    if (fornitureCalcolate.isEmpty) {
      throw new Exception("Non ci sono dati nelle Forniture calcolate! Controllare dati di input.")
    }

    logger.warn(s"Calcolo forniture e salvataggio in tabella: ${fornitureProcessiGasDao.tableName} su partizione $dataCalcolo")
    // dettaglio forniture e processi (non raggruppate per codice fiscale)
    fornitureProcessiGasDao.write(fornitureCalcolate)

    // flag per il caricamento in delta o full
    var caricamentoInDeltaFlag = false
    // forniture da caricare: o tutte le forniture calcolate o il suo delta
    val fornitureDaCaricareDs = if (interval == PortaleConsumiArgs.intervalFull || maxDocumentiScritturaDelta < 0) {
      logger.warn(s"Calcolo full forniture (interval: $interval, maxDocumentiScritturaDelta: $maxDocumentiScritturaDelta)")
      fornitureCalcolate
    } else {
      logger.warn(s"Calcolo delta forniture")
      val deltaFornitureDs = fornitureProcessiGasDao.calcolaDelta()
      logger.warn(s"Conteggio documenti per modalita' scrittura (limite doc scrittura delta: $maxDocumentiScritturaDelta)")
      val conteggioDocumenti = deltaFornitureDs.select(col(FornitureProcessiGasSchema.codice_fiscale),col(FornitureProcessiGasSchema.codice_pdr)).dropDuplicates().count()
      logger.warn(s"Conteggio documenti: $conteggioDocumenti")
      if (conteggioDocumenti > maxDocumentiScritturaDelta) {
        logger.warn(s"Numero documenti da caricare superiore a limite di caricamento DELTA. Caricamento in modalita' FULL")
        fornitureCalcolate
      } else {
        logger.warn(s"Numero documenti da caricare entro limite di caricamento DELTA. Caricamento in modalita' DELTA")
        caricamentoInDeltaFlag = true
        fornitureCalcolate.unpersist()
        deltaFornitureDs
      }
    }

    val fornitureMongo = convertiInStrutturaMongo(fornitureDaCaricareDs)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    fornitureCalcolate.unpersist()

    if (caricamentoInDeltaFlag) {
      logger.warn(s"Scrittura in MongoDb - DELTA")
      new FornitureGasMongoDbDao().write(fornitureMongo)
    } else {
      logger.warn(s"Scrittura in MongoDb con OVERWRITE - FULL")
      new FornitureGasMongoDbDao().write(fornitureMongo, overwrite = true)
    }
    fornitureMongo.unpersist()

    logger.warn("Pulizia dati calcolo obsoleti")
    fornitureProcessiGasDao.cancellaDatiPrecedentiUltimaEsecuzione()

    logger.warn("Fine processo Forniture Gas")
  }

  def leggiDatiInput(fromTimestamp: Timestamp, dataCalcoloTs: Timestamp): Map[String, DataFrame] = {
    Map(
      "forniture" -> new RcugasFornituraPDao().read(fromTimestamp).toDF().persist(),
      "cliente" -> new RcugasClientefinalePDao().readValidati().toDF(),
      "residenza" -> new RcugasResidenzaPDao().readResidenzeForniture().toDF(),
      "pdr" -> new RcugasPdrPDao().read().toDF(),
      "prelievi" -> new RcugasPdrDatiprelievoPDao().readPdrCategoria().toDF().persist(),
      "misuratori" -> new RcugasPdrMisuratorePDao().readMisuratoriInstallati().toDF().persist(),
      "indirizzi" -> new RcugasIndirizziPDao().read().toDF(),
      "venditori" -> new RcugasVenditorePDao().read().toDF(),
      "aziende" -> new VRcuAziendaPDao().read().toDF(),
      "connessioni" -> new RcugasConnessioniDistrPDao().readUltimoAggiornamento().toDF().persist(),
      "distributori" -> new VRcugasDistributorePDao().readAttivi().toDF(),
      "processi" -> new PrtSwgPDao().readProssimiSwitchPDR(dataCalcoloTs).toDF().persist(),
      "offerte" -> new RcugasCodiceOffertaPDao().readAttivi().toDF(),
      "vulnerabilita" -> new TdgVulnPDao().readAttivi().toDF()
    )
  }

  /** *
   * A partire dai dati di input calcola il dettaglio forniture - processi
   */
  def calcolaForniture(
                        forniture: DataFrame,
                        cliente: DataFrame,
                        residenza: DataFrame,
                        pdr: DataFrame,
                        prelievi: DataFrame,
                        misuratori: DataFrame,
                        indirizzi: DataFrame,
                        venditori: DataFrame,
                        aziende: DataFrame,
                        connessioni: DataFrame,
                        distributori: DataFrame,
                        processi: DataFrame,
                        offerte: DataFrame,
                        vulnerabilita: DataFrame,
                        dataCalcolo: String
                      ): Dataset[FornitureProcessiGasModel] = {
    val colFornituraAttiva = "_tmp_fornitura_attiva"
    val colClienteAttivo = "_tmp_cliente_attivo"
    val colPdrAttivo = "_tmp_pdr_attivo"
    val colIdCliente = "_tmp_cf_cliente"

    forniture
      .join(cliente, forniture(RcugasFornituraPSchema.n_id_cliente) === cliente(RcugasClientefinalePSchema.n_id_cliente), "INNER")
      // mantiene solo clienti con almeno una fornitura attiva
      .withColumn(colFornituraAttiva, when(forniture(RcugasFornituraPSchema.d_data_fine).isNull, 1).otherwise(0))
      .withColumn(colClienteAttivo, sum(col(colFornituraAttiva)).over(Window.partitionBy(forniture(RcugasFornituraPSchema.n_id_cliente))))
      .where(col(colClienteAttivo) > 0)
      // elimina forniture chiuse se pdr ha solo forniture chiuse per un cliente
      .withColumn(colIdCliente, coalesce(col(RcugasClientefinalePSchema.t_codice_fiscale), col(RcugasClientefinalePSchema.t_partita_iva)))
      .withColumn(colPdrAttivo, sum(col(colFornituraAttiva)).over(Window.partitionBy(col(colIdCliente), forniture(RcugasFornituraPSchema.n_id_pdr))))
      .where(col(colPdrAttivo) > 0)
      // aggiungi dettagli forniture
      .join(residenza, forniture(RcugasFornituraPSchema.n_id_fornitura) === residenza(RcugasResidenzaPSchema.n_id_fornitura), "LEFT")
      .join(pdr, forniture(RcugasFornituraPSchema.n_id_pdr) === pdr(RcugasPdrPSchema.n_id_pdr), "LEFT")
      .join(prelievi, forniture(RcugasFornituraPSchema.n_id_pdr) === prelievi(RcugasPdrDatiprelievoPSchema.n_id_pdr), "LEFT")
      .join(misuratori, forniture(RcugasFornituraPSchema.n_id_pdr) === misuratori(RcugasPdrMisuratorePSchema.n_id_pdr), "LEFT")
      .join(indirizzi, forniture(RcugasFornituraPSchema.n_indirizzo_fornitura) === indirizzi(RcugasIndirizziPSchema.n_id), "LEFT")
      .join(venditori, forniture(RcugasFornituraPSchema.n_id_vend) === venditori(RcugasVenditorePSchema.n_id_venditore), "LEFT")
      .join(aziende, venditori(RcugasVenditorePSchema.n_id_azienda) === aziende(VRcuAziendaPSchema.n_id_azienda), "LEFT")
      .join(connessioni, pdr(RcugasPdrPSchema.t_codice_pdr) === connessioni(RcugasConnessioniDistrPSchema.t_codice_pdr), "LEFT")
      .join(distributori, connessioni(RcugasConnessioniDistrPSchema.n_id_distr) === distributori(VRcugasDistributorePSchema.n_id_distributore), "LEFT")
      .join(processi, pdr(RcugasPdrPSchema.t_codice_pdr) === processi(PrtSwgPSchema.t_codice_pdr), "LEFT")
      .join(offerte, forniture(RcugasFornituraPSchema.n_id_fornitura) === offerte(RcugasCodiceOffertaPSchema.n_id_fornitura), "LEFT")
      .join(vulnerabilita,
        forniture(RcugasFornituraPSchema.n_id_cliente) === vulnerabilita(TdgVulnPSchema.n_id_cliente)
          and forniture(RcugasFornituraPSchema.n_id_pdr) === vulnerabilita(TdgVulnPSchema.n_id_pdr),
        "LEFT")
      .select(
        cliente(RcugasClientefinalePSchema.t_codice_fiscale) as FornitureProcessiGasSchema.codice_fiscale,
        cliente(RcugasClientefinalePSchema.t_nome) as FornitureProcessiGasSchema.nome,
        cliente(RcugasClientefinalePSchema.t_cognome) as FornitureProcessiGasSchema.cognome,
        cliente(RcugasClientefinalePSchema.t_partita_iva) as FornitureProcessiGasSchema.p_iva,
        cliente(RcugasClientefinalePSchema.t_ragione_sociale) as FornitureProcessiGasSchema.ragione_sociale,
        pdr(RcugasPdrPSchema.t_cod_tipo_pdr) as FornitureProcessiGasSchema.tipo_pdr,
        pdr(RcugasPdrPSchema.t_codice_pdr) as FornitureProcessiGasSchema.codice_pdr,
        forniture(RcugasFornituraPSchema.n_id_fornitura) as FornitureProcessiGasSchema.codice_fornitura,
        forniture(RcugasFornituraPSchema.d_data_inizio) as FornitureProcessiGasSchema.data_inizio_fornitura,
        forniture(RcugasFornituraPSchema.d_data_fine) as FornitureProcessiGasSchema.data_fine_fornitura,
        forniture(RcugasFornituraPSchema.d_aggiornamento) as FornitureProcessiGasSchema.data_aggiornamento,
        forniture(RcugasFornituraPSchema.t_tipo_fornitura) as FornitureProcessiGasSchema.tipo_fornitura,
        indirizzi(RcugasIndirizziPSchema.t_cap) as FornitureProcessiGasSchema.cap,
        indirizzi(RcugasIndirizziPSchema.t_comune) as FornitureProcessiGasSchema.comune,
        indirizzi(RcugasIndirizziPSchema.t_provincia) as FornitureProcessiGasSchema.provincia,
        indirizzi(RcugasIndirizziPSchema.t_toponimo) as FornitureProcessiGasSchema.toponimo_Indirizzo,
        indirizzi(RcugasIndirizziPSchema.t_nomestrada) as FornitureProcessiGasSchema.nome_strada,
        indirizzi(RcugasIndirizziPSchema.t_civico) as FornitureProcessiGasSchema.civico,
        indirizzi(RcugasIndirizziPSchema.t_nazione) as FornitureProcessiGasSchema.nazione,
        prelievi(RcugasPdrDatiprelievoPSchema.t_cod_cat_uso) as FornitureProcessiGasSchema.categoria_uso,
        misuratori(RcugasPdrMisuratorePSchema.t_classe_misuratore) as FornitureProcessiGasSchema.classe_misuratore,
        misuratori(RcugasPdrMisuratorePSchema.n_coeff_correzione) as FornitureProcessiGasSchema.coefficiente_conversione,
        misuratori(RcugasPdrMisuratorePSchema.t_matricola_misuratore) as FornitureProcessiGasSchema.matricola_misuratore,
        misuratori(RcugasPdrMisuratorePSchema.t_data_inst_misuratore) as FornitureProcessiGasSchema.data_di_decorrenza_gdm,
        aziende(VRcuAziendaPSchema.t_piva) as FornitureProcessiGasSchema.p_iva_cc,
        aziende(VRcuAziendaPSchema.t_rag_soc) as FornitureProcessiGasSchema.ragione_sociale_cc,
        distributori(VRcugasDistributorePSchema.t_rag_soc) as FornitureProcessiGasSchema.ragione_sociale_distributore,
        residenza(RcugasResidenzaPSchema.t_residenza) as FornitureProcessiGasSchema.residente,
        processi(PrtSwgPSchema.d_data_decorrenza) as FornitureProcessiGasSchema.data_di_decorrenza_switch,
        offerte(RcugasCodiceOffertaPSchema.t_codice_offerta) as FornitureProcessiGasSchema.codice_offerta,
        when(vulnerabilita(TdgVulnPSchema.n_id_tdg_vuln).isNotNull, "Y").otherwise("N") as FornitureProcessiGasSchema.cliente_vulnerabile,
        processi(PrtSwgPSchema.t_codice_pdr),
        lit(dataCalcolo) as FornitureProcessiGasSchema.data_calcolo
      )
      .withColumn(FornitureProcessiGasSchema.id_processo_gdm, lit("PRO001"))
      .withColumn(FornitureProcessiGasSchema.tipo_processo_gdm, lit("cambio_gdm"))
      .withColumn(FornitureProcessiGasSchema.data_inizio_processo_gdm, lit(null))
      .withColumn(FornitureProcessiGasSchema.data_fine_processo_gdm, lit(null))
      .withColumn(FornitureProcessiGasSchema.note_gdm, lit("note"))
      .withColumn(FornitureProcessiGasSchema.in_corso_gdm, when(
        col(FornitureProcessiGasSchema.data_di_decorrenza_gdm) >= col(FornitureProcessiGasSchema.data_calcolo), lit(true)
      ).otherwise(lit(false)))
      .withColumn(FornitureProcessiGasSchema.id_processo_switch, lit("PRO002"))
      .withColumn(FornitureProcessiGasSchema.tipo_processo_switch, lit("switch"))
      .withColumn(FornitureProcessiGasSchema.data_inizio_processo_switch, lit(null))
      .withColumn(FornitureProcessiGasSchema.data_fine_processo_switch, lit(null))
      .withColumn(FornitureProcessiGasSchema.note_switch, lit("note"))
      .withColumn(FornitureProcessiGasSchema.in_corso_switch, when(
        col(PrtSwgPSchema.t_codice_pdr).isNull, lit(false)).otherwise(lit(true)))
      // Calcola hash dalla concatenazione di tutte le colonne nella tabella
      // ad esclusione della colonna di hashcode e della data di calcolo
      .withColumn(FornitureProcessiGasSchema.hashcode, hash(concat_ws("", FornitureProcessiGasSchema.getValues
        .filter(_ != FornitureProcessiGasSchema.hashcode.toString)
        .filter(_ != FornitureProcessiGasSchema.data_calcolo.toString)
        .map(el => col(el)): _*)))
      .select(FornitureProcessiGasSchema.getValues.map(el => col(el)): _*)
      .as[FornitureProcessiGasModel]
  }

  /** *
   * Converte il dettaglio delle forniture - processi raggruppandoli per codice fiscale con la struttura
   * finale richiesta per la collezione MongoDB
   */
  def convertiInStrutturaMongo(ds: Dataset[FornitureProcessiGasModel]): Dataset[FornitureGasMongoDbModel] = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    ds.map(r =>
      // Inizializza struttura finale per ogni fornitura
        FornituraProcessi(
          codice_fiscale = r.codice_fiscale,
          codice_pdr = r.codice_pdr,
          anagrafica = Anagrafica(
            nome = if (r.nome != null) r.nome else "",
            cognome = if (r.cognome != null) r.cognome else "",
            p_iva = if (r.p_iva != null) r.p_iva else "",
            ragione_sociale = if (r.ragione_sociale != null) r.ragione_sociale else ""
          ),
          fornitura = Fornitura(
            cap = if (r.cap != null) r.cap else "",
            categoria_uso = if (r.categoria_uso != null) r.categoria_uso else "",
            civico = if (r.civico != null) r.civico else "",
            classe_misuratore = if (r.classe_misuratore != null) r.classe_misuratore else "",
            codice_fornitura = if (r.codice_fornitura != null) r.codice_fornitura else "",
            coefficiente_conversione = if (r.coefficiente_conversione != null) r.coefficiente_conversione else "",
            comune = if (r.comune != null) r.comune else "",
            data_inizio_fornitura = if (r.data_inizio_fornitura != null) dateFormat.format(r.data_inizio_fornitura) else "",
            data_fine_fornitura = if (r.data_fine_fornitura != null) dateFormat.format(r.data_fine_fornitura) else "",
            matricola_misuratore = if (r.matricola_misuratore != null) r.matricola_misuratore else "",
            nazione = if (r.nazione != null) r.nazione else "",
            nome_strada = if (r.nome_strada != null) r.nome_strada else "",
            p_iva_cc = if (r.p_iva_cc != null) r.p_iva_cc else "",
            provincia = if (r.provincia != null) r.provincia else "",
            ragione_sociale_cc = if (r.ragione_sociale_cc != null) r.ragione_sociale_cc else "",
            ragione_sociale_distributore = if (r.ragione_sociale_distributore != null) r.ragione_sociale_distributore else "",
            residente = if (r.residente != null) r.residente else "",
            tipo_fornitura = if (r.tipo_fornitura != null) r.tipo_fornitura else "",
            tipo_pdr = if (r.tipo_pdr != null) r.tipo_pdr else "",
            toponimo_Indirizzo = if (r.toponimo_Indirizzo != null) r.toponimo_Indirizzo else "",
            codice_offerta = if (r.codice_offerta != null) r.codice_offerta else "",
            cliente_vulnerabile = if (r.cliente_vulnerabile != null) r.cliente_vulnerabile else ""
          ),
          processi = {
            val result = Array(
              Processo(
                id_processo = if (r.id_processo_gdm != null) r.id_processo_gdm else "",
                data_inizio_processo = if (r.data_inizio_processo_gdm != null) dateFormat.format(r.data_inizio_processo_gdm) else "",
                data_fine_processo = if (r.data_fine_processo_gdm != null) dateFormat.format(r.data_fine_processo_gdm) else "",
                data_di_decorrenza = if (r.data_di_decorrenza_gdm != null) dateFormat.format(r.data_di_decorrenza_gdm) else "",
                in_corso = if (r.in_corso_gdm != null) r.in_corso_gdm else "",
                note = if (r.note_gdm != null) r.note_gdm else "",
                tipo_processo = if (r.tipo_processo_gdm != null) r.tipo_processo_gdm else ""
              ))
            if (r.data_di_decorrenza_switch != null) {
              // inserisci processo di switch solo se presente
              result :+ Processo(
                id_processo = if (r.id_processo_switch != null) r.id_processo_switch else "",
                data_inizio_processo = if (r.data_inizio_processo_switch != null) dateFormat.format(r.data_inizio_processo_switch) else "",
                data_fine_processo = if (r.data_fine_processo_switch != null) dateFormat.format(r.data_fine_processo_switch) else "",
                data_di_decorrenza = if (r.data_di_decorrenza_switch != null) dateFormat.format(r.data_di_decorrenza_switch) else "",
                in_corso = if (r.in_corso_switch != null) r.in_corso_switch else "",
                note = if (r.note_switch != null) r.note_switch else "",
                tipo_processo = if (r.tipo_processo_switch != null) r.tipo_processo_switch else ""
              )
            } else {
              result
            }
          }
        ))
      // raggruppa per codice fiscale e pdr e crea lista delle forniture attive e passate per ogni PDR
      // anagrafica e processi sono uguali per ogni fornitura, quindi prendiamo la prima occorrenza
      .groupBy(FornitureProcessiGasSchema.codice_fiscale, FornitureProcessiGasSchema.codice_pdr)
      .agg(
        first("anagrafica") as FornitureGasMongoDbSchema.anagrafica,
        sort_array(collect_list("fornitura")) as FornitureGasMongoDbSchema.forniture,
        first("processi") as FornitureGasMongoDbSchema.processi
      )
      .as[FornitureProcessi]
      .map(r => PdrProcessi(
        codice_fiscale = r.codice_fiscale,
        anagrafica = r.anagrafica,
        pdr = Pdr(
          codice_pdr = r.codice_pdr,
          forniture = r.forniture,
          processi = r.processi
        )
      ))
      // raggruppa per codice fiscale e unisci tutti i PDR in una lista finale
      .groupBy(FornitureProcessiGasSchema.codice_fiscale,FornitureGasMongoDbSchema.pdr + "." + FornitureGasMongoDbSchema.codice_pdr)
      .agg(
        first(FornitureGasMongoDbSchema.anagrafica) as FornitureGasMongoDbSchema.anagrafica,
        sort_array(collect_list(FornitureGasMongoDbSchema.pdr)) as FornitureGasMongoDbSchema.pdr
      )
      .withColumn(FornitureGasMongoDbSchema._id, concat(col(FornitureProcessiGasSchema.codice_fiscale),lit("_"),col(FornitureProcessiGasSchema.codice_pdr)))
      .withColumn(FornitureGasMongoDbSchema.id, concat(col(FornitureProcessiGasSchema.codice_fiscale),lit("_"),col(FornitureProcessiGasSchema.codice_pdr)))
        .drop(col(FornitureProcessiGasSchema.codice_pdr))
      .as[FornitureGasMongoDbModel]
  }
}
