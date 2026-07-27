package it.eng.au.portaleConsumi.flow

import it.eng.au.portaleConsumi.dao.hive.cmg_gas._
import it.eng.au.portaleConsumi.dao.hive.misuregas.{CalcoloMisureGasDao, FornitureMisureGasDao, FornitureProcessiGasDao}
import it.eng.au.portaleConsumi.dao.hive.switch_gas.PrtVtg6PDao
import it.eng.au.portaleConsumi.dao.mongodb.forniture.{MisureGas33MMongoDbDao, MisureGas3MMongoDbDao, MisureGasMongoDbDao}
import it.eng.au.portaleConsumi.model.flow.misure.{FornitureMisureGasArricchiteModel, FornitureMisureGasDeltaModel, FornitureMisureGasGruppoModel, MisureGasModel}
import it.eng.au.portaleConsumi.model.hive.misuregas._
import it.eng.au.portaleConsumi.model.mongodb.forniture._
import it.eng.au.portaleConsumi.schema.flow.misure.{FornitureMisureGasArricchiteSchema, FornitureMisureGasDeltaSchema, FornitureMisureGasGruppoSchema, MisureGasSchema}
import it.eng.au.portaleConsumi.schema.misuregas._
import it.eng.au.portaleConsumi.schema.mongodb.forniture.MisureGasMongoDbSchema
import it.eng.au.portaleConsumi.utility.args.PortaleConsumiArgs
import it.eng.au.portaleConsumi.utility.common.Costanti._
import it.eng.au.portaleConsumi.utility.common.MongoDbHelper.bsonValue
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.bson.{BsonArray, BsonDocument}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class MisureGasFlow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  private val dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val annoMeseFormatter = DateTimeFormatter.ofPattern("yyyyMM")

  private val dataCalcoloDefault = "2020-01-01"

  /** *
   * Esegue processo Misure Gas
   * Prima carica i dati delle misure nella tabella forniture_misure_gas e poi esegue i processi di scrittura su database finale
   *
   * @param flowArgsConfig parametri del processo
   */
  def run(flowArgsConfig: PortaleConsumiArgs): Unit = {
    logger.warn("Inizio processo Misure Gas")
    // inizializzazione variabili
    val fornitureMisureGasDao = new FornitureMisureGasDao()

    val dataCalcolo = flowArgsConfig.runDay
    val dataCalcoloString = dataCalcolo.format(dayFormatter)

    // lettura dati proprietà e calcolo intervalli
    /*
    Il numero dei mesi dal passato viene preso da parametro, ma per calcolo del delta si prende anche il mese precedente (-1)
    Il numero dei mesi di split indica la divisione tra il processo 3M e quello 33M:
    il 33M va da mese di intervallo massimo passato e arriva a mese split -1
    il 3M va da mese split a mese del calcolo
     */
    // numero di mesi in cui andare nel passato al massimo
    val numMesiNelPassato = Environment.getProperty("params.job.months_interval_max").toInt
    // numero mesi nel passato in cui dividere il processo 3M da quello 33M
    val numMesiSplit = Environment.getProperty("params.job.months_interval_split").toInt
    // numero mesi nel passato dove arrivare con il calcolo a 33M (il mese di split va sul 3M)
    val numMesiSplitPrecedente = numMesiSplit + 1
    // numero mesi massimo meno un mese per calcolare delta
    val numMesiPerCalcoloDelta = numMesiNelPassato + 1

    logger.warn(s"Calcolo intervallo processo")
    val annoMeseCalcolo = dataCalcolo.format(annoMeseFormatter)
    val annoMesePassatoRichiesto = dataCalcolo.minusMonths(numMesiNelPassato).format(annoMeseFormatter) //inizio intervallo per 33M
    val annoMeseMassimaProfondita = dataCalcolo.minusMonths(numMesiPerCalcoloDelta).format(annoMeseFormatter) //con mese aggiuntivo per delta
    val annoMeseSplit = dataCalcolo.minusMonths(numMesiSplit).format(annoMeseFormatter) //annomese split, da includere in processo 3M
    val annoMeseSplitPrecedente = dataCalcolo.minusMonths(numMesiSplitPrecedente).format(annoMeseFormatter) //annomese intervallo finale per il 33M
    logger.warn(
      s"""
         |Data calcolo del processo odierno: $dataCalcoloString
         |Anno mese di calcolo: $annoMeseCalcolo
         |Anno mese passato massimo: $annoMesePassatoRichiesto
         |Anno mese di split tra processi: $annoMeseSplit
         |""".stripMargin)
    logger.warn(s"Inizio aggiornamento misure gas per il periodo: $annoMesePassatoRichiesto - $annoMeseCalcolo")

    if (!flowArgsConfig.skipMisure) {
      logger.warn("Inizio aggiornamento misure")
      // leggi forniture con periodo attivazione compreso nel periodo di calcolo
      val fornitureUltimoCalcolo = new FornitureProcessiGasDao().leggiUltimoCalcolo()
      val fornitureDs = filtraFornitureNelPeriodo(fornitureUltimoCalcolo, annoMesePassatoRichiesto, annoMeseCalcolo)

      //leggi i dati di input
      val tsPassato = Timestamp.valueOf("2020-01-01 00:00:00") //non sembra si possa leggere in delta, 1.0 non lo fa
      val tsDataCalcolo = Timestamp.valueOf(dataCalcoloString + " 00:00:00")
      logger.warn(s"Lettura misure con data caricamento: $tsPassato - $tsDataCalcolo")
      val misureDs = leggiDatiMisure(annoMeseMassimaProfondita, tsPassato, tsDataCalcolo)

      val fornitureMisureDs = associaMisureConForniture(fornitureDs, misureDs)

      val misureRaggruppate = calcolaPrioritaPerGruppo(fornitureMisureDs, dataCalcoloString)
        .persist()

      val misureRiempimentoInizioFineFornitura = calcolaMisureRiempimentoInizioFineFornitura(
        misureRaggruppate, fornitureDs, annoMeseMassimaProfondita, annoMeseCalcolo, tsDataCalcolo)

      val misureRiempimentoMensileDaGiornaliero = calcolaMisureRiempimentoMensiliDaGiornalieri(misureRaggruppate)

      val misureTotali = misureRaggruppate
        .union(misureRiempimentoInizioFineFornitura)
        .union(misureRiempimentoMensileDaGiornaliero)

      val misureArricchite = calcolaDelta(misureTotali, dataCalcoloString)

      logger.warn(s"Scrittura misure in tabella ${fornitureMisureGasDao.tableName}")
      fornitureMisureGasDao.write(misureArricchite)

      // Pulizia tabelle
      logger.warn(s"Cancellazione dati obsoleti da tabella ${fornitureMisureGasDao.tableName}: " +
        s"dati con data calcolo inferiore a: $dataCalcoloString " +
        s"e anno mesi inferiori a: $annoMesePassatoRichiesto")
      fornitureMisureGasDao.cancellaDatiPrecedenti(annoMesePassatoRichiesto, dataCalcoloString)

    } else {
      logger.warn("Parametro SkipMisure valorizzato 'true': non aggiornare le misure")
    }

    flowArgsConfig.interval match {
      case PortaleConsumiArgs.intervalShort =>
        logger.warn(s"Avvio processo 3M")
        eseguiProcesso(flowArgsConfig, new MisureGas3MMongoDbDao(), annoMeseSplit, annoMeseCalcolo, PROCESSO_3M)
      case PortaleConsumiArgs.intervalLong =>
        logger.warn(s"Avvio processo 33M")
        eseguiProcesso(flowArgsConfig, new MisureGas33MMongoDbDao(), annoMesePassatoRichiesto, annoMeseSplitPrecedente, PROCESSO_33M)
      case PortaleConsumiArgs.intervalFull =>
        logger.warn(s"Avvio processo FULL")
        logger.warn(s"Avvio processo 3M")
        eseguiProcesso(flowArgsConfig, new MisureGas3MMongoDbDao(), annoMeseSplit, annoMeseCalcolo, PROCESSO_3M)
        logger.warn(s"Avvio processo 33M")
        eseguiProcesso(flowArgsConfig, new MisureGas33MMongoDbDao(), annoMesePassatoRichiesto, annoMeseSplitPrecedente, PROCESSO_33M)
      case _ => throw new IllegalArgumentException(s"Parametro interval ${flowArgsConfig.interval} non gestito")
    }

    logger.warn("Fine processo Misure Gas")
  }

  /** *
   * Esegue il processo su un periodo da inizio a fine (3M o 33M)
   */
  def eseguiProcesso(flowArgsConfig: PortaleConsumiArgs, dbDao: MisureGasMongoDbDao, annoMeseInizio: String,
                     annoMeseFine: String, tipoProcesso: String): Unit = {
    logger.warn(s"Intervallo calcolo I: $annoMeseInizio >= I >= $annoMeseFine")
    //variabili
    val fornitureMisureGasDao = new FornitureMisureGasDao()
    val calcoloMisureGasDao = new CalcoloMisureGasDao()
    //parametri
    val dataCalcoloString = flowArgsConfig.runDay.format(dayFormatter)
    //properties
    val maxDocumentiDelta = Environment.getProperty("params.mongodb.max_doc_delta_misure").toInt

    logger.warn("Calcolo numero forniture da aggiornare")
    val ultimaData = calcoloMisureGasDao.ultimaDataCalcolo(tipoProcesso)
    val dataUltimoCalcoloMisure = ultimaData match {
      case Some(x) => x
      case None => dataCalcoloDefault
    }

    logger.warn(s"Calcolo numero forniture da aggiornare nell'intervallo $annoMeseInizio >= I >= $annoMeseFine con data di caricamento maggiore uguale a: $dataUltimoCalcoloMisure")
    val fornitureDaAggiornare = fornitureMisureGasDao
      .fornitureDaAggiornare(annoMeseInizio, annoMeseFine, dataUltimoCalcoloMisure)
      .persist()

    val conteggioFornitureDaAggiornare = fornitureDaAggiornare.count()
    logger.warn(s"Numero di forniture da aggiornare: $conteggioFornitureDaAggiornare (max in delta: $maxDocumentiDelta)")

    val misureCompletePeriodo = fornitureMisureGasDao.leggiMisure(annoMeseInizio, annoMeseFine)

    if (conteggioFornitureDaAggiornare == 0) {
      logger.warn(s"Nessun aggiornamento da eseguire.")
    } else {
      val misurePeriodo = if (conteggioFornitureDaAggiornare <= maxDocumentiDelta) {
        logger.warn(s"Aggiornamento in modalita' DELTA; lettura di sole le misure delle forniture da aggiornare")
        misureCompletePeriodo
          .join(fornitureDaAggiornare,
            misureCompletePeriodo(FornitureMisureGasArricchiteSchema.codice_fornitura) === fornitureDaAggiornare(FornitureMisureGasArricchiteSchema.codice_fornitura),
            "LEFT_SEMI")
          .selectExpr(FornitureMisureGasArricchiteSchema.getValues: _*)
          .as[FornitureMisureGasArricchiteModel]
      } else {
        logger.warn(s"Aggiornamento in modalità FULL; lettura di tutte le misure")
        misureCompletePeriodo
      }

      val misureUtilizzate = misurePeriodo
        .where(col(FornitureMisureGasArricchiteSchema.usata_per_calcolo) === 1)

      val misureFinali = categorizzaMisure(misureUtilizzate)

      val fornitureMisureCompleteDs = raggruppaMisurePerFornitura(misureFinali)

      val misureGasMongoDb = convertiInStrutturaMongo(fornitureMisureCompleteDs)

      // scrivi su mongo
      if (conteggioFornitureDaAggiornare <= maxDocumentiDelta) {
        logger.warn("Scrittura su MongoDB in modalità DELTA")
        dbDao.write(misureGasMongoDb)
      } else {
        logger.warn("Scrittura su MongoDB in modalità FULL")
        dbDao.write(misureGasMongoDb, overwrite = true)
      }
    } //FINE if aggiornamento MongoDB

    logger.warn(s"Aggiornamento tabella data di calcolo: ${calcoloMisureGasDao.tableName}")
    val nuovoRecordCalcolo = CalcoloMisureGasModel(
      processo = tipoProcesso,
      data_calcolo = dataCalcoloString,
      ts_esecuzione = Timestamp.valueOf(LocalDateTime.now())
    )
    logger.warn(s"$nuovoRecordCalcolo")
    calcoloMisureGasDao.aggiornaDataCalcolo(nuovoRecordCalcolo)

    logger.warn(s"Fine calcolo su intervallo $annoMeseInizio >= I >= $annoMeseFine")
  }

  // FUNZIONI DI CALCOLO

  def filtraFornitureNelPeriodo(ds: Dataset[FornitureProcessiGasModel], annoMeseInizio: String, annoMeseFine: String): Dataset[FornitureProcessiGasModel] = {
    ds
      .where(date_format(col(FornitureProcessiGasSchema.data_inizio_fornitura), "yyyyMM") <= annoMeseFine)
      .where(col(FornitureProcessiGasSchema.data_fine_fornitura).isNull or
        date_format(col(FornitureProcessiGasSchema.data_fine_fornitura), "yyyyMM") >= annoMeseInizio)
  }

  /** *
   * Legge dati di input delle misure a partire dal anno e mese specificao in limiteAnnoMese e postume ad ultimoCalcoloMisureTs
   *
   * @param limiteAnnoMese        annomese di partenza lettura
   * @param ultimoCalcoloMisureTs timestamp di partenza di lettura
   * @param dataCalcoloMisureTs   timestamp limite di lettura
   */
  def leggiDatiMisure(limiteAnnoMese: String, ultimoCalcoloMisureTs: Timestamp, dataCalcoloMisureTs: Timestamp): Dataset[MisureGasModel] = {
    new PrtCmgRglPDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs)
      .union(new PrtCmgRmlPDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs))
      .union(new PrtCmgRmvPDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs))
      .union(new PrtCmgTalPDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs))
      .union(new PrtCmgTavPDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs))
      .union(new PrtCmgTglPDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs))
      .union(new PrtCmgTmlPDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs))
      .union(new PrtVtg6PDao().readNuoveMisure(limiteAnnoMese, ultimoCalcoloMisureTs, dataCalcoloMisureTs))
  }

  /** *
   * Associa forniture e misure sulla base del codice PDR e del periodo di attivita' della fornitura
   */
  def associaMisureConForniture(fornitureDs: Dataset[FornitureProcessiGasModel],
                                misureDs: Dataset[MisureGasModel]): Dataset[FornitureMisureGasModel] = {

    //misure associate a forniture attive/esistenti
    val fornitureMisure = fornitureDs.join(misureDs,
      fornitureDs(FornitureProcessiGasSchema.codice_pdr) === misureDs(MisureGasSchema.codice_pdr),
      "INNER")
      .where(fornitureDs(FornitureProcessiGasSchema.data_inizio_fornitura) <= misureDs(MisureGasSchema.data_lettura))
      .where(fornitureDs(FornitureProcessiGasSchema.data_fine_fornitura).isNull or (
        fornitureDs(FornitureProcessiGasSchema.data_fine_fornitura) >= misureDs(MisureGasSchema.data_lettura)))
      .select(
        fornitureDs(FornitureProcessiGasSchema.codice_fiscale) as FornitureMisureGasSchema.codice_fiscale,
        fornitureDs(FornitureProcessiGasSchema.p_iva) as FornitureMisureGasSchema.p_iva,
        fornitureDs(FornitureProcessiGasSchema.codice_pdr) as FornitureMisureGasSchema.codice_pdr,
        fornitureDs(FornitureProcessiGasSchema.codice_fornitura) as FornitureMisureGasSchema.codice_fornitura,
        misureDs(MisureGasSchema.flusso) as FornitureMisureGasSchema.flusso,
        misureDs(MisureGasSchema.lettura) as FornitureMisureGasSchema.lettura,
        misureDs(MisureGasSchema.data_lettura) as FornitureMisureGasSchema.data_lettura,
        misureDs(MisureGasSchema.motivazione) as FornitureMisureGasSchema.motivazione,
        misureDs(MisureGasSchema.data_caricamento) as FornitureMisureGasSchema.data_caricamento,
        misureDs(MisureGasSchema.annomese) as FornitureMisureGasSchema.annomese
      )
      .as[FornitureMisureGasModel]

    // mantieni le misure che hanno un PDR nella lista forniture ma nessuna fornitura attiva
    // le misure servono per il calcolo delta e per lo storico misure; non sono scritte su MongoDB

    // prendo le misure che hanno data lettura inferiore a minima data inizio fornitura per PDR (escluse da condizione precedente)
    val pdrPrimaFornituraAttiva = fornitureDs
      .groupBy(FornitureProcessiGasSchema.codice_pdr)
      .agg(
        min(FornitureProcessiGasSchema.data_inizio_fornitura).as(FornitureProcessiGasSchema.data_inizio_fornitura)
      )

    val misureSenzaFornitura = misureDs.join(pdrPrimaFornituraAttiva,
      misureDs(MisureGasSchema.codice_pdr) === pdrPrimaFornituraAttiva(FornitureProcessiGasSchema.codice_pdr),
      "INNER")
      .where(misureDs(MisureGasSchema.data_lettura) < pdrPrimaFornituraAttiva(FornitureProcessiGasSchema.data_inizio_fornitura))
      .select(
        lit(null) as FornitureMisureGasSchema.codice_fiscale,
        lit(null) as FornitureMisureGasSchema.p_iva,
        misureDs(MisureGasSchema.codice_pdr) as FornitureMisureGasSchema.codice_pdr,
        lit(null) as FornitureMisureGasSchema.codice_fornitura,
        misureDs(MisureGasSchema.flusso) as FornitureMisureGasSchema.flusso,
        misureDs(MisureGasSchema.lettura) as FornitureMisureGasSchema.lettura,
        misureDs(MisureGasSchema.data_lettura) as FornitureMisureGasSchema.data_lettura,
        misureDs(MisureGasSchema.motivazione) as FornitureMisureGasSchema.motivazione,
        misureDs(MisureGasSchema.data_caricamento) as FornitureMisureGasSchema.data_caricamento,
        misureDs(MisureGasSchema.annomese) as FornitureMisureGasSchema.annomese
      )
      .as[FornitureMisureGasModel]

    fornitureMisure
      .union(misureSenzaFornitura)
  }

  /** *
   * Raggruppa tutte le misure nel formato per gruppo e priorità (FornitureMisureGasGruppoModel)
   *
   * Il formato atteso prevede un codice gruppo e priorità: nelle fasi successive vengono scelte le misure sulla base della
   * loro priorità per cui si prenderanno le misure che, per lo stesso gruppo, hanno un valore di priorità piu' alto
   * (priorità piu' alta -> intero piu' piccolo)
   */
  def calcolaPrioritaPerGruppo(fornitureMisureGasDs: Dataset[FornitureMisureGasModel], dataCalcolo: String): Dataset[FornitureMisureGasGruppoModel] = {
    val misureRettifiche = calcolaPrioritaMisure(fornitureMisureGasDs, dataCalcolo)

    misureRettifiche
      .selectExpr(FornitureMisureGasGruppoSchema.getValues: _*)
      .as[FornitureMisureGasGruppoModel]
  }

  /** *
   * Aggiungi misure nulle mensili nel primo e ultimo mese di una fornitura (con bassa priorità) se non esistono misure
   * per l'anno mese di inizio e fine.
   * Per la misura di fine fornitura inserisce nella data di lettura la data fine della fornitura (come job versione 1.0)
   *
   * Codice riempimento: 1
   *
   * @param misureDs    dataset con misure
   * @param fornitureDs dataset forniture per estrarre mese di inizio e fine fornitura
   * @return
   */
  def calcolaMisureRiempimentoInizioFineFornitura(misureDs: Dataset[FornitureMisureGasGruppoModel],
                                                  fornitureDs: Dataset[FornitureProcessiGasModel],
                                                  annoMeseInizio: String, annoMeseFine: String, dataCalcolo: Timestamp)
  : Dataset[FornitureMisureGasGruppoModel] = {
    val annoMeseFormat = "yyyyMM"

    // nomi colonne di supporto
    val annoMeseInizioFornituraCol = "_tmp_am_f_inizio"
    val annoMeseFineFornituraCol = "_tmp_am_f_fine"
    val misuraMinCol = "_tmp_min_annomese"
    val misuraMaxCol = "_tmp_max_annomese"
    val inserimentoInizioCol = "_tmp_inizio_forn"
    val inserimentoFineCol = "_tmp_fine_forn"

    // massimo e minimo anno-mese di misura per ogni fornitura
    val misureMese = misureDs
      .select(FornitureMisureGasGruppoSchema.codice_fornitura, FornitureMisureGasGruppoSchema.annomese)
      .groupBy(FornitureMisureGasGruppoSchema.codice_fornitura)
      .agg(
        min(FornitureMisureGasGruppoSchema.annomese).as(misuraMinCol),
        max(FornitureMisureGasGruppoSchema.annomese).as(misuraMaxCol)
      )

    fornitureDs
      .join(misureMese,
        fornitureDs(FornitureProcessiGasSchema.codice_fornitura) === misureMese(FornitureMisureGasGruppoSchema.codice_fornitura),
        "LEFT"
      )
      .withColumn(annoMeseInizioFornituraCol, date_format(col(FornitureProcessiGasSchema.data_inizio_fornitura), annoMeseFormat))
      .withColumn(annoMeseFineFornituraCol, date_format(col(FornitureProcessiGasSchema.data_fine_fornitura), annoMeseFormat))
      // inserimentoInizioCol, inserimentoFineCol: valorizzati solo se prima e ultima misura differenti da inizio e fine fornitura
      .withColumn(inserimentoInizioCol,
        when(col(misuraMinCol).isNull or col(annoMeseInizioFornituraCol) =!= col(misuraMinCol), col(annoMeseInizioFornituraCol)).otherwise(lit(null)))
      .withColumn(inserimentoFineCol,
        when(col(misuraMaxCol).isNull or col(annoMeseFineFornituraCol) =!= col(misuraMaxCol), col(annoMeseFineFornituraCol)).otherwise(lit(null)))
      .select(
        fornitureDs(FornitureProcessiGasSchema.codice_fornitura) as FornitureMisureGasSchema.codice_fornitura,
        col(FornitureProcessiGasSchema.codice_fiscale) as FornitureMisureGasSchema.codice_fiscale,
        col(FornitureProcessiGasSchema.p_iva) as FornitureMisureGasSchema.p_iva,
        col(FornitureProcessiGasSchema.codice_pdr) as FornitureMisureGasSchema.codice_pdr,
        // esplodi in piu righe per aggiungere misura di inizio e fine (se necessario)
        explode(array(col(inserimentoInizioCol), col(inserimentoFineCol))) as FornitureMisureGasSchema.annomese,
        lit(null) as FornitureMisureGasSchema.lettura,
        when(col(FornitureMisureGasSchema.annomese) === col(annoMeseFineFornituraCol), col(FornitureProcessiGasSchema.data_fine_fornitura))
          .otherwise(lit(null)) as FornitureMisureGasSchema.data_lettura,
        lit(null) as FornitureMisureGasSchema.motivazione,
        fornitureDs(FornitureProcessiGasSchema.data_aggiornamento) as FornitureMisureGasSchema.data_caricamento,
        lit(_MAN) as FornitureMisureGasSchema.flusso
      )
      .where(col(FornitureMisureGasSchema.annomese).isNotNull)
      .where(col(FornitureMisureGasSchema.annomese) > annoMeseInizio)
      .where(col(FornitureMisureGasSchema.annomese) <= annoMeseFine)
      .selectExpr(FornitureMisureGasSchema.getValues: _*)
      .as[FornitureMisureGasModel]
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_periodo_competenza, col(FornitureMisureGasSchema.annomese))
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_flusso, lit(GRUPPO_MISURE_MENSILI))
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_priorita, lit(9))
      .withColumn(FornitureMisureGasGruppoSchema.data_calcolo, lit(null))
      .withColumn(FornitureMisureGasGruppoSchema.riempimento, lit(1))
      .selectExpr(FornitureMisureGasGruppoSchema.getValues: _*)
      .as[FornitureMisureGasGruppoModel]
  }

  /** *
   * Calcola misure mensili a partire dai dati giornalieri
   * Da usare quando mancano i dati mensili nella categoria misure mensili (e non altre frequenze)
   * Codice riempimento: 2
   */
  def calcolaMisureRiempimentoMensiliDaGiornalieri(misureDs: Dataset[FornitureMisureGasGruppoModel]): Dataset[FornitureMisureGasGruppoModel] = {
    val rowNumberCol = "_tmp_row_number"
    val esisteMisureMensileCol = "_tmp_misura_mensile"

    val windowsMisuraMensile = Window.partitionBy(
      FornitureMisureGasGruppoSchema.codice_fornitura,
      FornitureMisureGasGruppoSchema.annomese
    )
    val windowsPrioritaMisuraGiornaliera = Window.partitionBy(
      FornitureMisureGasGruppoSchema.codice_fornitura,
      FornitureMisureGasGruppoSchema.annomese
    ).orderBy(
      col(FornitureMisureGasGruppoSchema.data_lettura).desc_nulls_last,
      col(FornitureMisureGasGruppoSchema.gruppo_priorita))

    misureDs
      .withColumn(esisteMisureMensileCol, sum(when(col(FornitureMisureGasGruppoSchema.flusso).isin(TGL, RGL), 0)
        .otherwise(1))
        .over(windowsMisuraMensile))
      .where(col(esisteMisureMensileCol) === 0)
      .where(col(FornitureMisureGasGruppoSchema.flusso).isin(TGL, RGL))
      .withColumn(rowNumberCol, row_number().over(windowsPrioritaMisuraGiornaliera))
      .where(col(rowNumberCol) === 1)
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_periodo_competenza, col(FornitureMisureGasSchema.annomese))
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_flusso, lit(GRUPPO_MISURE_MENSILI))
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_priorita, lit(8))
      .withColumn(FornitureMisureGasGruppoSchema.riempimento, lit(2))
      .selectExpr(FornitureMisureGasGruppoSchema.getValues: _*)
      .as[FornitureMisureGasGruppoModel]
  }

  /** *
   * Raggruppa misure per tipologia ed indica la priorità della misura
   *
   * @param fornitureMisureGasDs tutte le misure
   * @return
   */
  def calcolaPrioritaMisure(fornitureMisureGasDs: Dataset[FornitureMisureGasModel], dataCalcolo: String)
  : Dataset[FornitureMisureGasGruppoModel] = {
    fornitureMisureGasDs
      // raggruppamento per flussi
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_flusso,
        when(col(FornitureMisureGasSchema.flusso).isin(TML, RML), GRUPPO_MISURE_MENSILI)
          .when(col(FornitureMisureGasSchema.flusso).isin(TGL, RGL), GRUPPO_MISURE_GIORNALIERE)
          .when(col(FornitureMisureGasSchema.flusso).isin(VTG, RMV), GRUPPO_MISURE_VOLTURE)
          .when(col(FornitureMisureGasSchema.flusso).isin(TAL, TAV), GRUPPO_MISURE_AUTOLETTURE)
      )
      // colonna che identifica periodo di competenza tra misure e rettifiche
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_periodo_competenza,
        when(col(FornitureMisureGasSchema.flusso).isin(TML, RML), col(FornitureMisureGasSchema.annomese))
          .when(col(FornitureMisureGasSchema.flusso).isin(TGL, RGL), col(FornitureMisureGasSchema.data_lettura))
          .when(col(FornitureMisureGasSchema.flusso).isin(VTG, RMV), col(FornitureMisureGasSchema.annomese))
          .when(col(FornitureMisureGasSchema.flusso).isin(TAL, TAV), col(FornitureMisureGasSchema.annomese))
      )
      // priorità tra i flussi raggruppati; valore minore implica priorità maggiore
      .withColumn(FornitureMisureGasGruppoSchema.gruppo_priorita,
        when(col(FornitureMisureGasSchema.flusso) === RML, lit(0))
          .when(col(FornitureMisureGasSchema.flusso) === TML, lit(1))
          .when(col(FornitureMisureGasSchema.flusso) === RGL, lit(0))
          .when(col(FornitureMisureGasSchema.flusso) === TGL, lit(1))
          .when(col(FornitureMisureGasSchema.flusso) === RMV, lit(0))
          .when(col(FornitureMisureGasSchema.flusso) === VTG, lit(1))
          .when(col(FornitureMisureGasSchema.flusso) === TAL, lit(0))
          .when(col(FornitureMisureGasSchema.flusso) === TAV, lit(0))
      )
      .withColumn(FornitureMisureGasGruppoSchema.data_calcolo, lit(dataCalcolo))
      .withColumn(FornitureMisureGasGruppoSchema.riempimento, lit(0))
      .selectExpr(FornitureMisureGasGruppoSchema.getValues: _*)
      .as[FornitureMisureGasGruppoModel]
  }


  /** *
   * Filtra i dati mantenendo solo quelli con massima priorita' e successivamente calcola il delta
   */
  def calcolaDelta(fornitureMisureGasGruppoDs: Dataset[FornitureMisureGasGruppoModel], dataCalcolo: String): Dataset[FornitureMisureGasArricchiteModel] = {
    // colonne aggiuntive per raggruppamenti
    val gruppoRowNumberCol = "_tmp_rn"
    val gruppoDeltaCol = "_tmp_gruppo_delta"
    val partitionDeltaCol = "_tmp_partition_delta"
    val gruppoMisuraPrecedenteCol = "_tmp_misura_precedente"

    // window di raggruppamento: mantieni a parità di gruppo la misura con valore di priorità minore
    val gruppoCategoriaWindow = Window
      .partitionBy(
        FornitureMisureGasGruppoSchema.codice_fornitura,
        FornitureMisureGasGruppoSchema.gruppo_flusso,
        FornitureMisureGasGruppoSchema.gruppo_periodo_competenza
      )
      .orderBy(
        col(FornitureMisureGasGruppoSchema.gruppo_priorita),
        col(FornitureMisureGasGruppoSchema.data_lettura).desc_nulls_last,
        col(FornitureMisureGasGruppoSchema.data_caricamento).desc_nulls_last
      )

    val gruppoDeltaWindow = Window
      .partitionBy(
        partitionDeltaCol, //se mensile codice pdr, giornaliero codice fornitura,
        gruppoDeltaCol
      )
      .orderBy(col(FornitureMisureGasSchema.annomese).asc_nulls_last, col(FornitureMisureGasSchema.data_lettura).asc_nulls_last)

    val misureCalcolo = fornitureMisureGasGruppoDs
      // mantieni misura con priorità massima a parità di gruppo flusso
      .withColumn(gruppoRowNumberCol, row_number().over(gruppoCategoriaWindow))
      .where(col(gruppoRowNumberCol) === 1)
      // il delta è calcolato sulle misure mensili + volture + autoletture (gruppo 1) e giornaliere a parte (gruppo 2)
      .withColumn(gruppoDeltaCol,
        when(col(FornitureMisureGasGruppoSchema.gruppo_flusso).isin(GRUPPO_MISURE_VOLTURE, GRUPPO_MISURE_MENSILI, GRUPPO_MISURE_AUTOLETTURE), lit(GRUPPO_DELTA_MENSILI))
          .when(col(FornitureMisureGasGruppoSchema.gruppo_flusso).isin(GRUPPO_MISURE_GIORNALIERE), lit(GRUPPO_DELTA_GIORNALIERI))
          .otherwise(lit(null)))
      //il delta e' calcolato per pdr nelle misure mensili, per codice fornitura per giornalieri
      .withColumn(partitionDeltaCol, when(
        col(gruppoDeltaCol).isin(GRUPPO_DELTA_MENSILI), col(FornitureMisureGasGruppoSchema.codice_pdr)) // + GRUPPO_DELTA_AUTOLETTURE se dividere autoletture
        .otherwise(col(FornitureMisureGasGruppoSchema.codice_fornitura)))
      // calcola delta con misura precedente
      .withColumn(gruppoMisuraPrecedenteCol, lag(col(FornitureMisureGasSchema.lettura), 1, null)
        .over(gruppoDeltaWindow))
      .withColumn(FornitureMisureGasArricchiteSchema.delta_misure,
        when(col(FornitureMisureGasSchema.lettura).isNull and col(FornitureMisureGasSchema.flusso).isin(RMV, RGL, RML, TAL, TAV, VTG), lit(0)) //rettifiche possono avere lettura null
          .when(col(FornitureMisureGasSchema.lettura).isNull, lit(null)) //se lettura inserita manuale allora delta nullo (da 1.0)
          .when(col(gruppoMisuraPrecedenteCol).isNull, 0)
          .otherwise(col(FornitureMisureGasSchema.lettura) - col(gruppoMisuraPrecedenteCol)))
      // completamento dati
      .withColumn(FornitureMisureGasArricchiteSchema.usata_per_calcolo, lit(1))
      .withColumn(FornitureMisureGasArricchiteSchema.data_calcolo, lit(dataCalcolo))
      .withColumn(FornitureMisureGasArricchiteSchema.codice_fiscale, coalesce(
        col(FornitureMisureGasGruppoSchema.codice_fiscale), col(FornitureMisureGasGruppoSchema.p_iva)))
      .withColumn(FornitureMisureGasArricchiteSchema.cod_pdr,
        substring(col(FornitureMisureGasArricchiteSchema.codice_pdr), 7, 3))
      .selectExpr(FornitureMisureGasArricchiteSchema.getValues: _*)
      .as[FornitureMisureGasArricchiteModel]

    val misureScartate = fornitureMisureGasGruppoDs
      // mantieni misura con priorità massima a parità di gruppo flusso
      .withColumn(gruppoRowNumberCol, row_number().over(gruppoCategoriaWindow))
      .where(col(gruppoRowNumberCol) > 1)
      .withColumn(FornitureMisureGasArricchiteSchema.delta_misure, lit(null))
      .withColumn(FornitureMisureGasArricchiteSchema.usata_per_calcolo, lit(0))
      .withColumn(FornitureMisureGasArricchiteSchema.data_calcolo, lit(dataCalcolo))
      .withColumn(FornitureMisureGasArricchiteSchema.codice_fiscale, coalesce(
        col(FornitureMisureGasGruppoSchema.codice_fiscale), col(FornitureMisureGasGruppoSchema.p_iva)))
      .withColumn(FornitureMisureGasArricchiteSchema.cod_pdr,
        substring(col(FornitureMisureGasArricchiteSchema.codice_pdr), 7, 3))
      .selectExpr(FornitureMisureGasArricchiteSchema.getValues: _*)
      .as[FornitureMisureGasArricchiteModel]

    misureCalcolo
      .union(misureScartate)
  }

  /** *
   * Associa le misure alle categorie finali MongoDB (autoletture, misure mensili, volture, misure giornaliere e misure altre frequenze)
   * e aggiunge i valori agli attributi
   */
  def categorizzaMisure(misure: Dataset[FornitureMisureGasArricchiteModel]): Dataset[FornitureMisureGasDeltaModel] = {
    /*
    moltiplica le misure che sono da salvare in categorie differenti creando una "lista di destinazione" che verrà successivamente esplosa
    autoletture -> autoletture e mensili
    mensili -> mensili e altre frequenze
    volture -> volture e mensili
    giornaliere -> giornaliere
    mensili da giornaliere -> mensili
    */
    val categoriaMisureCol = "_tmp_categorie"

    val misureCategorie = misure
      .withColumn(categoriaMisureCol,
        when(col(FornitureMisureGasGruppoSchema.gruppo_flusso) === GRUPPO_MISURE_VOLTURE, typedLit(Seq(CATEGORIA_MISURE_VOLTURA, CATEGORIA_MISURE_MENSILI)))
          .when(col(FornitureMisureGasGruppoSchema.gruppo_flusso) === GRUPPO_MISURE_AUTOLETTURE, typedLit(Seq(CATEGORIA_MISURE_AUTOLETTURA, CATEGORIA_MISURE_MENSILI)))
          .when(col(FornitureMisureGasGruppoSchema.gruppo_flusso) === GRUPPO_MISURE_MENSILI and
            col(FornitureMisureGasGruppoSchema.flusso).isin(TML, RML, _MAN), typedLit(Seq(CATEGORIA_MISURE_AF, CATEGORIA_MISURE_MENSILI)))
          // misure mensili calcolate da giornaliero
          .when(col(FornitureMisureGasGruppoSchema.gruppo_flusso) === GRUPPO_MISURE_MENSILI and
            col(FornitureMisureGasGruppoSchema.flusso).isin(TGL, RGL), typedLit(Seq(CATEGORIA_MISURE_MENSILI)))
          .when(col(FornitureMisureGasGruppoSchema.gruppo_flusso) === GRUPPO_MISURE_GIORNALIERE, typedLit(Seq(CATEGORIA_MISURE_GIORNALIERE)))
          .otherwise(typedLit(Seq()))
      )
      .withColumn(FornitureMisureGasDeltaSchema.categoria_misura, explode(col(categoriaMisureCol)))
      .withColumn(FornitureMisureGasDeltaSchema.tipo_misura,
        when(col(FornitureMisureGasGruppoSchema.flusso) === TAL, TIPO_MISURE_AUTOLETTURA)
          .when(col(FornitureMisureGasGruppoSchema.flusso) === TAV, TIPO_MISURE_AUTOLETTURA)
          .when(col(FornitureMisureGasGruppoSchema.flusso) === TML, TIPO_LETTURA_PERIODICA)
          .when(col(FornitureMisureGasGruppoSchema.flusso) === RML, TIPO_LETTURA_RETTIFICA)
          .when(col(FornitureMisureGasGruppoSchema.flusso) === TGL, TIPO_LETTURA_PERIODICA)
          .when(col(FornitureMisureGasGruppoSchema.flusso) === RGL, TIPO_LETTURA_RETTIFICA)
          .when(col(FornitureMisureGasGruppoSchema.flusso) === VTG, TIPO_MISURE_VOLTURA)
          .when(col(FornitureMisureGasGruppoSchema.flusso) === RMV, TIPO_LETTURA_RETTIFICA_VOLTURA)
          .otherwise(TIPO_RIEMPIMENTO)
      )
      .persist()

    /*
    Per le misure mensili e' possibile che ci siano piu' misure in un dato anno-mese. Nel caso prendere misura con data_lettura, data_caricamento massima
    Ricalcolare il delta tra le misure selezionate; per la misura con il primo anno-mese viene mantenuto il delta gia' calcolato
     */
    val colRowNumberMisureMensili = "_tmp_rn_misure_mensili"
    val windowMisureMensili = Window
      .partitionBy(FornitureMisureGasGruppoSchema.codice_fornitura, FornitureMisureGasGruppoSchema.annomese)
      .orderBy(
        col(FornitureMisureGasGruppoSchema.data_lettura).desc_nulls_last,
        col(FornitureMisureGasGruppoSchema.data_caricamento).desc_nulls_last
      )

    val gruppoMisuraPrecedenteCol = "_tmp_misura_precedente_mensili"
    val gruppoDeltaWindow = Window.partitionBy(FornitureMisureGasDeltaSchema.codice_fornitura)
      .orderBy(FornitureMisureGasDeltaSchema.annomese)

    val misureMensili = misureCategorie
      .where(col(FornitureMisureGasDeltaSchema.categoria_misura) === CATEGORIA_MISURE_MENSILI)
      .withColumn(colRowNumberMisureMensili, row_number().over(windowMisureMensili))
      .where(col(colRowNumberMisureMensili) === 1)
      .withColumn(gruppoMisuraPrecedenteCol, lag(col(FornitureMisureGasDeltaSchema.lettura), 1, null)
        .over(gruppoDeltaWindow))
      // ricalcola delta, se prima misura allora usa il delta calcolato precedentemente
      .withColumn(FornitureMisureGasDeltaSchema.delta_misure,
        when(col(gruppoMisuraPrecedenteCol).isNull and col(FornitureMisureGasDeltaSchema.delta_misure).isNull, null)
          .when(col(gruppoMisuraPrecedenteCol).isNull, col(FornitureMisureGasDeltaSchema.delta_misure))
          .otherwise(col(FornitureMisureGasDeltaSchema.lettura) - col(gruppoMisuraPrecedenteCol)))
      .selectExpr(FornitureMisureGasDeltaSchema.getValues: _*)
      .as[FornitureMisureGasDeltaModel]

    misureCategorie
      .selectExpr(FornitureMisureGasDeltaSchema.getValues: _*)
      .as[FornitureMisureGasDeltaModel]
      .where(col(FornitureMisureGasDeltaSchema.categoria_misura) =!= CATEGORIA_MISURE_MENSILI)
      .union(misureMensili)
  }

  /** *
   * Raggruppa le misure per fornitura nelle categorie finali
   */
  def raggruppaMisurePerFornitura(misureGasDs: Dataset[FornitureMisureGasDeltaModel]): Dataset[MisuraDettaglio] = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    misureGasDs
      .where(col(MisureGasMongoDbSchema.codice_fornitura).isNotNull)
      // predispone per raggruppamento finale ogni misura con i suoi dettagli
      .map(r => MisuraDettaglioEstesa(
        codice_fornitura = r.codice_fornitura,
        pdr = r.codice_pdr,
        autoletture = if (r.categoria_misura == CATEGORIA_MISURE_AUTOLETTURA) {
          AutoletturaDettaglio(
            competenza_consumi = r.annomese,
            data_lettura = if (r.data_lettura == null) null else dateFormat.format(r.data_lettura),
            lettura_mese = if (r.lettura == null) null else r.lettura.toString + ".0",
            tipo_misura = r.tipo_misura
          )
        } else null,
        misure_giornaliere = if (r.categoria_misura == CATEGORIA_MISURE_GIORNALIERE) {
          GiornalieraDettaglio(
            competenza_consumi = r.annomese,
            data_lettura = if (r.data_lettura == null) null else dateFormat.format(r.data_lettura),
            delta_misure = if (r.delta_misure == null) null else r.delta_misure.toString + ".0",
            lettura_giorno = if (r.lettura == null) null else r.lettura.toString + ".0",
            tipo_misura = r.tipo_misura
          )
        } else null,
        misure_altre_frequenze = if (r.categoria_misura == CATEGORIA_MISURE_AF) {
          MensileDettaglio(
            competenza_consumi = r.annomese,
            data_lettura = if (r.data_lettura == null) null else dateFormat.format(r.data_lettura),
            delta_misure = if (r.delta_misure == null) null else r.delta_misure.toString + ".0",
            lettura_mese = if (r.lettura == null) null else r.lettura.toString + ".0",
            tipo_misura = r.tipo_misura
          )
        } else null,
        misure_mensili = if (r.categoria_misura == CATEGORIA_MISURE_MENSILI) {
          MensileDettaglio(
            competenza_consumi = r.annomese,
            data_lettura = if (r.data_lettura == null) null else dateFormat.format(r.data_lettura),
            delta_misure = if (r.delta_misure == null) null else r.delta_misure.toString + ".0",
            lettura_mese = if (r.lettura == null) null else r.lettura.toString + ".0",
            tipo_misura = r.tipo_misura
          )
        } else null,
        volture = if (r.categoria_misura == CATEGORIA_MISURE_VOLTURA) {
          VolturaDettaglio(
            competenza_consumi = r.annomese,
            data_lettura = if (r.data_lettura == null) null else dateFormat.format(r.data_lettura),
            lettura_misura = if (r.lettura == null) null else r.lettura.toString + ".0",
            tipo_misura = r.tipo_misura
          )
        } else null
      ))
      // raggruppa per categoria di misura
      .groupBy(MisureGasMongoDbSchema.codice_fornitura, MisureGasMongoDbSchema.pdr)
      .agg(
        sort_array(collect_list(col(MisureGasMongoDbSchema.autoletture))) as MisureGasMongoDbSchema.autoletture,
        sort_array(collect_list(col(MisureGasMongoDbSchema.misure_giornaliere))) as MisureGasMongoDbSchema.misure_giornaliere,
        sort_array(collect_list(col(MisureGasMongoDbSchema.misure_altre_frequenze))) as MisureGasMongoDbSchema.misure_altre_frequenze,
        sort_array(collect_list(col(MisureGasMongoDbSchema.misure_mensili))) as MisureGasMongoDbSchema.misure_mensili,
        sort_array(collect_list(col(MisureGasMongoDbSchema.volture))) as MisureGasMongoDbSchema.volture
      )
      .as[MisuraDettaglio]
  }

  /** *
   * Converte il dato in formato MisuraDettaglio nella struttura JSON finale.
   * Non si puo' utilizzare un dataset in quanto ad un documento possono mancare determinate misure (non tutti i PDR
   * hanno misure giornaliere)
   */
  def convertiInStrutturaMongo(fornitureMisureDs: Dataset[MisuraDettaglio]): RDD[BsonDocument] = {
    // non posso usare la case class perché se degli attributi sono nulli non devono essere scritti
    fornitureMisureDs
      .rdd
      .map(r => {
        val document = new BsonDocument()
        val misure = new BsonDocument()
        document.append(MisureGasMongoDbSchema._id, bsonValue(r.codice_fornitura))
        document.append(MisureGasMongoDbSchema.codice_fornitura, bsonValue(r.codice_fornitura))
        document.append(MisureGasMongoDbSchema.pdr, bsonValue(r.pdr))

        if (r.autoletture.length != 0) {
          val autoletture = new BsonArray()
          for (misura <- r.autoletture) {
            val lettura = new BsonDocument()
            lettura.append(MisureGasMongoDbSchema.competenza_consumi, bsonValue(misura.competenza_consumi))
            lettura.append(MisureGasMongoDbSchema.data_lettura, bsonValue(misura.data_lettura))
            lettura.append(MisureGasMongoDbSchema.lettura_mese, bsonValue(misura.lettura_mese))
            lettura.append(MisureGasMongoDbSchema.tipo_misura, bsonValue(misura.tipo_misura))
            autoletture.add(lettura)
          }
          misure.append(MisureGasMongoDbSchema.autoletture, autoletture)
        }

        if (r.misure_giornaliere.length != 0) {
          val misure_giornaliere = new BsonArray()
          for (misura <- r.misure_giornaliere) {
            val lettura = new BsonDocument()
            lettura.append(MisureGasMongoDbSchema.competenza_consumi, bsonValue(misura.competenza_consumi))
            lettura.append(MisureGasMongoDbSchema.data_lettura, bsonValue(misura.data_lettura))
            lettura.append(MisureGasMongoDbSchema.delta_misure, bsonValue(misura.delta_misure))
            lettura.append(MisureGasMongoDbSchema.lettura_giorno, bsonValue(misura.lettura_giorno))
            lettura.append(MisureGasMongoDbSchema.tipo_misura, bsonValue(misura.tipo_misura))
            misure_giornaliere.add(lettura)
          }
          misure.append(MisureGasMongoDbSchema.misure_giornaliere, misure_giornaliere)
        }

        if (r.misure_altre_frequenze.length != 0) {
          val misure_altre_frequenze = new BsonArray()
          for (misura <- r.misure_altre_frequenze) {
            val lettura = new BsonDocument()
            lettura.append(MisureGasMongoDbSchema.competenza_consumi, bsonValue(misura.competenza_consumi))
            lettura.append(MisureGasMongoDbSchema.data_lettura, bsonValue(misura.data_lettura))
            lettura.append(MisureGasMongoDbSchema.delta_misure, bsonValue(misura.delta_misure))
            lettura.append(MisureGasMongoDbSchema.lettura_mese, bsonValue(misura.lettura_mese))
            lettura.append(MisureGasMongoDbSchema.tipo_misura, bsonValue(misura.tipo_misura))
            misure_altre_frequenze.add(lettura)
          }
          misure.append(MisureGasMongoDbSchema.misure_altre_frequenze, misure_altre_frequenze)
        }

        if (r.misure_mensili.length != 0) {
          val misure_mensili = new BsonArray()
          for (misura <- r.misure_mensili) {
            val lettura = new BsonDocument()
            lettura.append(MisureGasMongoDbSchema.competenza_consumi, bsonValue(misura.competenza_consumi))
            lettura.append(MisureGasMongoDbSchema.data_lettura, bsonValue(misura.data_lettura))
            lettura.append(MisureGasMongoDbSchema.delta_misure, bsonValue(misura.delta_misure))
            lettura.append(MisureGasMongoDbSchema.lettura_mese, bsonValue(misura.lettura_mese))
            lettura.append(MisureGasMongoDbSchema.tipo_misura, bsonValue(misura.tipo_misura))
            misure_mensili.add(lettura)
          }
          misure.append(MisureGasMongoDbSchema.misure_mensili, misure_mensili)
        }

        if (r.volture.length != 0) {
          val volture = new BsonArray()
          for (misura <- r.volture) {
            val lettura = new BsonDocument()
            lettura.append(MisureGasMongoDbSchema.competenza_consumi, bsonValue(misura.competenza_consumi))
            lettura.append(MisureGasMongoDbSchema.data_lettura, bsonValue(misura.data_lettura))
            lettura.append(MisureGasMongoDbSchema.lettura_misura, bsonValue(misura.lettura_misura))
            lettura.append(MisureGasMongoDbSchema.tipo_misura, bsonValue(misura.tipo_misura))
            volture.add(lettura)
          }
          misure.append(MisureGasMongoDbSchema.volture, volture)
        }

        document.append(MisureGasMongoDbSchema.misure, misure)
        document
      }
      )
  }

}
