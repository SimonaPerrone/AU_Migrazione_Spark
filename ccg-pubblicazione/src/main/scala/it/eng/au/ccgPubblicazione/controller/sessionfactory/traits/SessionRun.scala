package it.eng.au.ccgPubblicazione.controller.sessionfactory.traits

import it.eng.au.ccgPubblicazione.args.FlowArgsConfig
import it.eng.au.ccgPubblicazione.controller.runnablefile.traits.RunnableAggregator
import it.eng.au.ccgPubblicazione.dao.request.{RequestFilterDao, RequestPdrDao}
import it.eng.au.ccgPubblicazione.schema.request.{RequestFilterSchema, RequestPdrSchema}
import it.eng.au.ccgPubblicazione.utility.Constants._
import it.eng.au.ccgPubblicazione.utility.Environment
import it.eng.au.ccgPubblicazione.utility.FileUtility.isBroadcast
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable
import it.eng.au.ccgPubblicazione.controller.sessionfactory.impl._

/** Contiene i valori e i metodi comuni a tutti gli oggetti finali ([[AggSession]], [[SbgSession]], [[CdpFinSession]], [[CdpRicSession]]) */
trait SessionRun {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  /** Sessione di lancio (AGG, SBG, CDP_FIN, CDP_RIC) */
  val session: String
  val sessionLog: String

  // Definizione degli oggetti che verranno utilizzati per la pubblicazione
  val idPdrElencoFlussi: RunnableAggregator
  val uddPdrElencoFlussi: RunnableAggregator
  val udbPdrElencoFlussi: RunnableAggregator
  val gestorePdrElencoFlussi: RunnableAggregator

  val idPdrElencoFlussiIncoerentiAB: RunnableAggregator
  val uddPdrElencoFlussiIncoerentiAB: RunnableAggregator
  val udbPdrElencoFlussiIncoerentiAB: RunnableAggregator
  val gestorePdrElencoFlussiIncoerentiAB: RunnableAggregator

  val idPdrElencoFlussiIncoerentiC: RunnableAggregator
  val uddPdrElencoFlussiIncoerentiC: RunnableAggregator
  val udbPdrElencoFlussiIncoerentiC: RunnableAggregator
  val gestorePdrElencoFlussiIncoerentiC: RunnableAggregator

  val idPdrElencoFlussiEsclusi: RunnableAggregator
  val uddPdrElencoFlussiEsclusi: RunnableAggregator
  val udbPdrElencoFlussiEsclusi: RunnableAggregator
  val gestorePdrElencoFlussiEsclusi: RunnableAggregator

  // Campi delle partite ive di riferimento nella tabella dei consumi
  val pivaUddFieldConsumption: String
  val pivaUdbFieldConsumption: String
  val pivaIdFieldConsumption: String
  val pivaGestoreFieldConsumption: String

  // Campi PdR nella tabella dei consumi e nella tabella dei flussi
  val pdrFieldConsumption: String
  val pdrFieldValidation: String

  // Campi da pubblicare
  val fieldsConsumptionRequestRunnable: List[String]

  // Campo id richiesta
  val idRichiestaFields: String

  // Campi della tabella dei consumi utilizzati nelle richieste di tipo filtro
  val filtroFieldCodProfConsumption: String
  val filtroFiledCodRemiConsumption: String
  val filtroFiledTrattamentoConsumption: String

  /** Inizializza Spark e setta le properties aggiuntive utilizzate dal processo. */
  def setEnvironment(flowArgsConfig: FlowArgsConfig): Unit

  /** Legge l'ultimo executionid della tabella dei consumi (leggendo soltanto le partizioni CCG). */
  def readConsumptionWithLastPartition: (DataFrame, String)

  /**
   * Legge la tabella dei flussi validati utilizzando lo stesso executionid ([[partition]]) della tabella dei consumi
   *
   * @param partition executionid da leggere
   * @return tabella dei flussi validati
   */
  def readValidation(partition: String): DataFrame

  /**
   * Esecuzione del processo di CCG per la sessione selezionata. In particolare,
   *  1. Inizializza Spark e configura le properties aggiuntive utilizzate dal processo ([[setEnvironment]]));
   *  1. Legge le tabelle di richiesta PdR e richiesta filtro entrando con la data richiesta passata come input;
   *  1. Legge l'ultimo executionid della tabella dei consumi ([[readConsumptionWithLastPartition]]);
   *  1. Legge lo stesso executionid della tabella dei flussi validati ([[readValidation]]);
   *  1. Se attivate, e se le richieste PdR non sono vuote, esegue il processo [[runRunnableAggregator]] sulle richieste PdR;
   *  1. Se attivate, e se le richieste filtro non sono vuote, esegue il processo [[runRunnableAggregator]] sulle richieste filtro.
   *
   * @param flowArgsConfig parsed properties
   */
  def run(flowArgsConfig: FlowArgsConfig): Unit = {

    setEnvironment(flowArgsConfig)

    logger.warn(s"$sessionLog Request Type: ${flowArgsConfig.tipo}")
    logger.warn(s"$sessionLog Execution date: ${flowArgsConfig.dataRichiesta}")
    Environment.setDataRichiesta(flowArgsConfig.dataRichiesta)
    logger.warn(s"$sessionLog Process Type: ${flowArgsConfig.session}")
    Environment.setSessione(flowArgsConfig.session)

    //read request table
    val requestPdr = RequestPdrDao.getDf(session, flowArgsConfig.dataRichiesta)
    val requestFiltro = RequestFilterDao.getDf(session, flowArgsConfig.dataRichiesta)

    //read consumption and last partition to use
    val (consumption, partition) = readConsumptionWithLastPartition
    Environment.setConsumptionExecutionid(partition)

    //read validation with same consumption partition's
    val validation = readValidation(partition)

    //consumption.cache
    //validation.cache

    // Esecuzione della pubblicazione di tipo PDR per i quattro destinatari UDD, UDB, ID, GESTORE
    if (List(PDR, ALL).contains(flowArgsConfig.tipo)) {
      val countRequest = requestPdr.count
      logger.warn(s"$sessionLog Number of request pdr is $countRequest")

      if (countRequest > 0) {
        requestPdr.cache
        logger.warn(s"$sessionLog Run request pdr...")

        runRunnableAggregator(consumption, validation, requestPdr, UDD, PDR)
        runRunnableAggregator(consumption, validation, requestPdr, UDB, PDR)
        runRunnableAggregator(consumption, validation, requestPdr, ID, PDR)
        runRunnableAggregator(consumption, validation, requestPdr, GESTORE, PDR)

        logger.warn(s"$sessionLog End request pdr")
      }
    }

    // Esecuzione della pubblicazione di tipo FILTRO per i quattro destinatari UDD, UDB, ID, GESTORE
    if (List(FILTRO, ALL).contains(flowArgsConfig.tipo)) {
      val countRequest = requestFiltro.count
      logger.warn(s"$sessionLog Number of request filtro is $countRequest")

      if (countRequest > 0) {
        requestFiltro.cache
        logger.warn(s"$sessionLog Run request filtro...")

        runRunnableAggregator(consumption, validation, requestFiltro, UDD, FILTRO)
        runRunnableAggregator(consumption, validation, requestFiltro, UDB, FILTRO)
        runRunnableAggregator(consumption, validation, requestFiltro, ID, FILTRO)
        runRunnableAggregator(consumption, validation, requestFiltro, GESTORE, FILTRO)

        logger.warn(s"$sessionLog End request filtro")
      }
    }
  }

  /**
   * Esegue la procedura di pubblicazione per una tipologia di richieste ([[PDR]] o [[FILTRO]]) e per un determinato ruolo ([[UDD]], [[UDB]], [[ID]] o [[GESTORE]]) a partire dalle tabelle dei consumi [[consumption]] e dei flussi validati [[validation]]. In particolare,
   *  1. Filtriamo le richieste per ruolo ([[UDD]], [[UDB]], [[ID]] o [[GESTORE]]);
   *  1. Se vi è almeno una richiesta,
   *    1. nel caso di `tipo=PDR`,
   *      1. filtriamo i dataframe dei consumi e dei flussi validati selezionando i PdR richiesti ([[filterPdrInputTable]]);
   *      1. eseguiamo la pubblicazione CCG utilizzando tali dataframe ([[RunnableAggregator.run]]);
   *    1. nel caso di `tipo=FILTRO`,
   *      1. filtriamo i dataframe dei consumi e dei flussi validati utilizzando i filtri richiesti ([[filterFiltroInputTable]]);
   *      1. eseguiamo la procedura di pubblicazione CCG per le richieste di tipo incoerenti A+B e incoerenti C ([[runIncoerenti]]);
   *      1. eseguiamo la procedura di pubblicazione CCG per le richieste rimanenti ([[RunnableAggregator.run]]);
   *
   * @param consumption dataframe dei consumi
   * @param validation  dataframe dei flussi validati
   * @param request     dataframe delle richieste
   * @param ruolo       ruolo degli utenti di cui vogliamo effettuare la pubblicazione ([[UDD]], [[UDB]], [[ID]] o [[GESTORE]])
   * @param tipo        tipologia della richiesta da lavorare ([[PDR]] o [[FILTRO]])
   */
  def runRunnableAggregator(consumption: DataFrame, validation: DataFrame, request: DataFrame, ruolo: String, tipo: String): Unit = {
    //choose the object (runnable) to run based on the type of user and the piva that filter the consumption
    //incoerenti need only filter pdr
    val (pivaFilter, runnableAggregatorAggregati, runnableAggregatorIncoerentiAB, runnableAggregatorIncoerentiC, runnableAggregatorEsclusi) = ruolo match {
      case UDD => (pivaUddFieldConsumption, uddPdrElencoFlussi, uddPdrElencoFlussiIncoerentiAB, uddPdrElencoFlussiIncoerentiC, uddPdrElencoFlussiEsclusi)
      case UDB => (pivaUdbFieldConsumption, udbPdrElencoFlussi, udbPdrElencoFlussiIncoerentiAB, udbPdrElencoFlussiIncoerentiC, udbPdrElencoFlussiEsclusi)
      case ID => (pivaIdFieldConsumption, idPdrElencoFlussi, idPdrElencoFlussiIncoerentiAB, idPdrElencoFlussiIncoerentiC, idPdrElencoFlussiEsclusi)
      case GESTORE => (pivaGestoreFieldConsumption, gestorePdrElencoFlussi, gestorePdrElencoFlussiIncoerentiAB, gestorePdrElencoFlussiIncoerentiC, gestorePdrElencoFlussiEsclusi)
    }

    /** Dataframe delle richieste filtrato per ruolo ([[UDD]], [[UDB]], [[ID]] o [[GESTORE]]). */
    val filterRequest = request.filter(col(RequestPdrSchema.T_RUOLO) === ruolo)
    filterRequest.cache

    val countRequest = filterRequest.count
    logger.warn(s"$sessionLog Number of request for rule $ruolo is $countRequest")

    if (countRequest > 0) {
      /** Dataframe filtrato in base alla tipologia di richiesta ([[PDR]] o [[FILTRO]]). */
      val (consumptionFilter, validationFilter, requestWithoutIncoerenti) = tipo match {
        // Nel caso di richiesta PDR, semplicemente filtriamo i dataframe tramite il metodo filterPdrInputTable.
        case PDR =>
          filterPdrInputTable(consumption, validation, filterRequest, pivaFilter, ruolo)
        // Nel caso di richiesta FILTRO, filtriamo i dataframe tramite i filtri richiesti, poi eseguiamo la pubblicazione delle richieste di tipo incoerenti,
        // e infine eseguiamo la pubblicazione per le richieste rimanenti.
        case FILTRO =>
          val (consumptionFilterFiltro, validationFilterFiltro) = filterFiltroInputTable(consumption, validation, filterRequest, pivaFilter, ruolo)
          consumptionFilterFiltro.cache
          validationFilterFiltro.cache

          // Eseguiamo prima la procedura degli incoerenti A+B, C ed Esclusi
          runIncoerenti(filterRequest, consumptionFilterFiltro, validationFilterFiltro, runnableAggregatorIncoerentiAB, runnableAggregatorIncoerentiC, runnableAggregatorEsclusi, tipo)

          /** Dataframe dei consumi contente i record non legati a una richiesta incoerenti. */
          val notIncoerenti = consumptionFilterFiltro.filter(col(RequestFilterSchema.T_INCOERENTI).isNull || col(RequestFilterSchema.T_INCOERENTI) === "")
            .selectExpr(fieldsConsumptionRequestRunnable: _*)

          /** Richieste diverse dalle richieste incoerenti. */
          val requestWithoutIncoerenti = filterRequest.filter(col(RequestFilterSchema.T_INCOERENTI).isNull || col(RequestFilterSchema.T_INCOERENTI) === "")

          (notIncoerenti, validationFilterFiltro, requestWithoutIncoerenti)
      }

      // Eseguiamo la procedura di pubblicazione CCG per le richieste diverse da incoerenti
      runnableAggregatorAggregati.run(consumptionFilter, validationFilter, requestWithoutIncoerenti, tipo == PDR)
      filterRequest.unpersist
    }
  }

  /**
   * Esegue la pubblicazione CCG per le richieste di tipo incoerenti A+B e incoerenti C (solo richieste [[FILTRO]] e solo sessioni AGG e SBG). In particolare,
   *  1. I dataframe dei consumi e dei flussi validati vengono filtrati tramite le richieste di incoerenti A+B;
   *  1. Viene richiamato il metodo [[RunnableAggregator.run]] per eseguire la pubblicazione degli incoerenti A+B;
   *  1. Gli step 1 e 2 vengono ripetuti per gli incoerenti C.
   * @param filterRequest richieste di tipo incoerenti A+B o incoerenti C
   * @param consumptionFilterFiltro dataframe dei consumi filtrato
   * @param validationFilterFiltro dataframe dei flussi validati filtrato
   * @param runnableAggregatorIncoerentiAB oggetto di tipo [[RunnableAggregator]] che si occupa della pubblicazione incoerenti A+B
   * @param runnableAggregatorIncoerentiC oggetto di tipo [[RunnableAggregator]] che si occupa della pubblicazione incoerenti C
   * @param tipo tipologia della richiesta (nel caso di [[PDR]] il metodo non fa nulla
   */
  def runIncoerenti(
                     filterRequest: DataFrame
                     , consumptionFilterFiltro: DataFrame
                     , validationFilterFiltro: DataFrame
                     , runnableAggregatorIncoerentiAB: RunnableAggregator
                     , runnableAggregatorIncoerentiC: RunnableAggregator
                     , runnableAggregatorEsclusi: RunnableAggregator
                     , tipo: String
                   ): Unit = {
    // Gli incoerenti AB o C sono calcolati solo per le sessioni AGG e SBG
    if (List(AGG, SBG).contains(session)) {
      /** Contiene le richieste di tipo incoerenti AB. */
      val filterIncoerentiABRequest = filterRequest.filter(col(RequestFilterSchema.T_INCOERENTI) === incoerentiAB)
      val countIncoerentiAB = filterIncoerentiABRequest.count
      logger.warn(s"$sessionLog Number of request incoerenti AB is $countIncoerentiAB")
      if (countIncoerentiAB > 0) {
        /** Dataframe dei consumi filtrato tramite le richieste incoerenti AB. */
        val consumptionFilterFiltroIncoerentiAB = consumptionFilterFiltro.filter(col(RequestFilterSchema.T_INCOERENTI) === incoerentiAB)
          .selectExpr(fieldsConsumptionRequestRunnable: _*)

        /** Dataframe dei flussi filtrato tramite le richieste incoerenti AB. */
        val validatedFilterFiltroIncoerentiAB = validatedFilter(consumptionFilterFiltroIncoerentiAB, validationFilterFiltro)
        // Esecuzione dei processo di pubblicazione degli incoerenti AB
        runnableAggregatorIncoerentiAB.run(consumptionFilterFiltroIncoerentiAB, validatedFilterFiltroIncoerentiAB, filterIncoerentiABRequest, tipo == PDR)
      }
      /** Contiene le richieste di tipo incoerenti C. */
      val filterIncoerentiCRequest = filterRequest.filter(col(RequestFilterSchema.T_INCOERENTI) === incoerentiC)
      val countIncoerentiC = filterIncoerentiCRequest.count
      logger.warn(s"$sessionLog Number of request incoerenti C is $countIncoerentiC")
      if (countIncoerentiC > 0) {
        /** Dataframe dei consumi filtrato tramite le richieste incoerenti C. */
        val consumptionFilterFiltroIncoerentiC = consumptionFilterFiltro.filter(col(RequestFilterSchema.T_INCOERENTI) === incoerentiC)
          .selectExpr(fieldsConsumptionRequestRunnable: _*)

        val consumptionWithCoefficient = attachCoefficient(consumptionFilterFiltroIncoerentiC)
          .selectExpr(fieldsConsumptionRequestRunnable: _*)

        /** Dataframe dei flussi filtrato tramite le richieste incoerenti C. */
        val validatedFilterFiltroIncoerentiC = validatedFilter(consumptionWithCoefficient, validationFilterFiltro)
        // Esecuzione del processo di pubblicazione degli incoerenti C
        runnableAggregatorIncoerentiC.run(consumptionWithCoefficient, validatedFilterFiltroIncoerentiC, filterIncoerentiCRequest, tipo == PDR)
      }

      /** Contiene le richieste di tipo esclusi. */
      val filterIncoerentiEsclusiRequest = filterRequest.filter(col(RequestFilterSchema.T_INCOERENTI) === incoerentiEsclusi)
      val countIncoerentiEsclusi = filterIncoerentiEsclusiRequest.count
      logger.warn(s"$sessionLog Number of request incoerenti E is $countIncoerentiEsclusi")
      if (countIncoerentiEsclusi > 0) {
        /** Dataframe dei consumi filtrato tramite le richieste incoerenti Esclusi. */
        val consumptionFilterFiltroIncoerentiEsclusi = consumptionFilterFiltro.filter(col(RequestFilterSchema.T_INCOERENTI) === incoerentiEsclusi)
          .selectExpr(fieldsConsumptionRequestRunnable: _*)

        /** Dataframe dei flussi filtrato tramite le richieste incoerenti Esclusi. */
        val validatedFilterFiltroIncoerentiEsclusi = validatedFilter(consumptionFilterFiltroIncoerentiEsclusi, validationFilterFiltro)
        // Esecuzione dei processo di pubblicazione degli incoerenti Esclusi
        runnableAggregatorEsclusi.run(consumptionFilterFiltroIncoerentiEsclusi, validatedFilterFiltroIncoerentiEsclusi, filterIncoerentiEsclusiRequest, tipo == PDR)
      }
    }
  }

  //Used in SBG to get the correct coefficient from rcugas if not present in daily consumption
  def attachCoefficient(dailyConsumption: DataFrame): DataFrame = dailyConsumption


  /**
   * Filtra le tabelle dei consumi [[consumption]] e dei flussi validati [[validation]] utilizzando le richieste di tipo PDR presenti in [[request]].
   * @param consumption dataframe dei consumi
   * @param validation dataframe dei flussi validati
   * @param request dataframe delle richieste di tipo PdR
   * @param pivaFieldConsumption nome del campo della partita iva che effettua la richiesta
   * @param ruolo ruolo degli utenti di cui vogliamo effettuare la pubblicazione ([[UDD]], [[UDB]], [[ID]] o [[GESTORE]])
   * @return dataframe [[consumption]] e [[validation]] filtrati tramite [[request]], in aggiunta al dataframe [[request]] stesso.
   */
  def filterPdrInputTable(
                           consumption: DataFrame
                           , validation: DataFrame
                           , request: DataFrame
                           , pivaFieldConsumption: String
                           , ruolo: String
                         ): (DataFrame, DataFrame, DataFrame) = {

    val prepRequest = prepareRequestPdr(request)
    prepRequest.cache
    val requestBroadcast = (if (isBroadcast(prepRequest)) broadcast(prepRequest) else prepRequest)
    //      .filter(col(RequestPdrSchema.T_AMMISSIBILITA_PDR) === SI && col(RequestPdrSchema.T_AMMISSIBILITA_FILE) === SI)

    //    prepRequest.cache()
    //    logger.warn(s"$sessionLog prepRequest is ${prepRequest.count}")

    // [AU-549] Qui si potrebbe mettere una distinct della prepRequest senza la n_id_richiesta

    /** Join tra il dataframe dei consumi e le richieste di tipo PdR. Nel caso di ruolo [[GESTORE]], non vi è la condizione di join sulla partita iva. */
    val joinConsumptionRequest = ruolo match {
      case GESTORE => consumption.join(requestBroadcast, consumption(pdrFieldConsumption) === prepRequest(RequestPdrSchema.T_CODICE_PDR), "inner")
      case _ => consumption.join(requestBroadcast, consumption(pivaFieldConsumption) === prepRequest(RequestPdrSchema.T_PIVA) && consumption(pdrFieldConsumption) === prepRequest(RequestPdrSchema.T_CODICE_PDR), "inner")
    }

    //    joinConsumptionRequest.cache()
    //    logger.warn(s"$sessionLog joinConsumptionRequest is ${joinConsumptionRequest.count}")

    /** Dataframe dei consumi ulteriormente filtrato. */
    val consumptioFilter = consumptionFilterPdr(joinConsumptionRequest, pivaFieldConsumption)
    consumptioFilter.cache

    //    consumptioFilter.cache()
    //    logger.warn(s"$sessionLog consumptioFilter is ${consumptioFilter.count}")

    /** Dataframe dei flussi validati contenente soltanto i PdR presenti nel dataframe dei consumi filtrato. */
    val validateFilter = validatedFilter(consumptioFilter, validation)

    //    validateFilter.cache()
    //    logger.warn(s"$sessionLog validateFilter is ${validateFilter.count}")

    (consumptioFilter, validateFilter, request)
  }

  /**
   * Filtra i dataframe dei consumi e dei flussi validati utilizzando le richieste di tipo filtro.
   * @param consumption dataframe dei consumi
   * @param validation dataframe dei flussi
   * @param request dataframe delle richieste di tipo filtro
   * @param pivaFieldConsumption nome del campo della partita iva che effettua la richiesta
   * @param ruolo ruolo degli utenti di cui vogliamo effettuare la pubblicazione ([[UDD]], [[UDB]], [[ID]] o [[GESTORE]])
   * @return dataframe [[consumption]] e [[validation]] filtrati tramite [[request]].
   */
  def filterFiltroInputTable(
                              consumption: DataFrame
                              , validation: DataFrame
                              , request: DataFrame
                              , pivaFieldConsumption: String
                              , ruolo: String
                            ): (DataFrame, DataFrame) = {

    val prepRequest = prepareRequestFiltro(request)
    prepRequest.cache
    val requestBroadcast = if (isBroadcast(prepRequest)) broadcast(prepRequest) else prepRequest
    //      .filter(col(RequestPdrSchema.T_AMMISSIBILITA_PDR) === SI && col(RequestPdrSchema.T_AMMISSIBILITA_FILE) === SI)

    /** Join tra il dataframe dei consumi e le richieste di tipo filtro. Nel caso di ruolo [[GESTORE]], è necessario effettuare una crossJoin. */
    val joinConsumptionRequest = ruolo match {
      case GESTORE => consumption.crossJoin(requestBroadcast)
      case _ => consumption.join(requestBroadcast, consumption(pivaFieldConsumption) === prepRequest(RequestPdrSchema.T_PIVA), "inner")
    }

    /** Dataframe dei consumi filtrato tramite le richieste. */
    val consumptioFilter = consumptionFilterFiltro(joinConsumptionRequest, pivaFieldConsumption)
    consumptioFilter.cache

    /** Dataframe dei flussi validati contenente soltanto i PdR presenti nel dataframe dei consumi filtrato. */
    val validateFilter = validatedFilter(consumptioFilter, validation)

    (consumptioFilter, validateFilter)
  }

  /**
   * Prepara il dataframe delle richieste di tipo PdR per le successive join con le tabelle di consumo e dei flussi.
   * In particolare, andiamo a selezionare soltanto i record ammissibili (ammissbilità == 1)
   * @param request dataframe delle richieste di tipo PdR
   * @return dataframe delle richieste ammissibili di tipo PdR
   */
  def prepareRequestPdr(request: DataFrame): DataFrame = {
    request
      .filter(col(RequestPdrSchema.B_AMMISSIBILITA) === AMMISSIBILITA_SI_1)
      .select(
        RequestPdrSchema.N_ID_RICHIESTA,
        RequestPdrSchema.T_PIVA,
        RequestPdrSchema.T_CODICE_PDR,
        RequestPdrSchema.T_ANNO,
        RequestPdrSchema.T_MESE /*,
        RequestPdrSchema.D_DATA_RICHIESTA*/
      )
      .distinct
  }

  /**
   * Prepara il dataframe delle richieste di tipo filtro per le successive join con le tabelle di consumo e dei flussi.
   * In particolare, andiamo a applicare la funzione [[collect_set]] sulla colonna `T_CODPROFSTD`.
    * @param request dataframe delle richieste di tipo filtro
   * @return dataframe delle richieste di tipo filtro pronto per gli step successivi
   */
  def prepareRequestFiltro(request: DataFrame): DataFrame = {
    request
      .groupBy(
        RequestFilterSchema.N_ID_RICHIESTA,
        RequestFilterSchema.T_SERVIZIO,
        RequestFilterSchema.T_PROCESSO,
        //        RequestFilterSchema.D_DATA_RICHIESTA,
        RequestFilterSchema.T_ANNO,
        RequestFilterSchema.T_MESE,
        RequestFilterSchema.T_RUOLO,
        RequestFilterSchema.T_PIVA,
        RequestFilterSchema.T_COD_REMI,
        RequestFilterSchema.T_INCOERENTI,
        RequestFilterSchema.T_TRATTAMENTO,
        RequestFilterSchema.T_PIVA_UDD,
        RequestFilterSchema.T_PIVA_UDB,
        RequestFilterSchema.T_PIVA_ID
      )
      .agg(
        collect_set(RequestFilterSchema.T_CODPROFSTD).as(RequestFilterSchema.T_CODPROFSTD)
      )
  }

  //this override only agg session
  def filterAnnoMese(annoField: String, meseField: String): Column = lit(true)

  /**
   * Applica l'eventuale filto [[filterAnnoMese]] (implementato solo per AGG), rinomina alcune colonne e seleziona soltanto le colonne che utilizzeremo.
   * @param consumption dataframe dei consumi con le info delle richieste
   * @param pivaFieldConsumption nome del campo della partita iva che effettua la richiesta
   * @return dataframe dei consumi con le info delle richieste, eventualmente filtrato e pulito
   */
  def consumptionFilterPdr(consumption: DataFrame, pivaFieldConsumption: String): DataFrame = {
    //    val arrayStringsContains = udf((pdr: String, pdrs: mutable.WrappedArray[String]) => pdrs.contains(pdr))

    consumption
      .filter(
        filterAnnoMese(RequestPdrSchema.T_ANNO.toString, RequestPdrSchema.T_MESE.toString)
      )
      .withColumnRenamed(RequestPdrSchema.N_ID_RICHIESTA, idRichiestaFields)
      //      .withColumnRenamed(RequestPdrSchema.D_DATA_RICHIESTA, dataRichiestaFields)
      .withColumnRenamed(RequestPdrSchema.T_PIVA, pivaGestoreFieldConsumption)
      .selectExpr(fieldsConsumptionRequestRunnable: _*)
  }

  /**
   * Filtra il dataframe dei consumi utilizzando le informazioni contenute nelle richieste di tipo filtro.
   * @param consumption dataframe dei consumi con le info delle richieste
   * @param pivaFieldConsumption nome del campo della partita iva che effettua la richiesta
   * @return dataframe [[consumption]] filtrato tramite le richieste
   */
  def consumptionFilterFiltro(consumption: DataFrame, pivaFieldConsumption: String): DataFrame = {
    val arrayStringsContains = udf((cod_prof: String, cod_profs: mutable.WrappedArray[String]) => cod_profs.isEmpty || cod_profs.contains(cod_prof))

    val pivaFilter = Map(
      pivaUddFieldConsumption -> (col(RequestFilterSchema.T_PIVA_UDD).isNull || col(pivaUddFieldConsumption) === col(RequestFilterSchema.T_PIVA_UDD)),
      pivaUdbFieldConsumption -> (col(RequestFilterSchema.T_PIVA_UDB).isNull || col(pivaUdbFieldConsumption) === col(RequestFilterSchema.T_PIVA_UDB)),
      pivaIdFieldConsumption -> (col(RequestFilterSchema.T_PIVA_ID).isNull || col(pivaIdFieldConsumption) === col(RequestFilterSchema.T_PIVA_ID))
    ).updated(pivaFieldConsumption, lit(true))
      .values.reduce(_ && _)

    consumption
      .filter(
        arrayStringsContains(consumption(filtroFieldCodProfConsumption), col(RequestFilterSchema.T_CODPROFSTD)) &&
          filterAnnoMese(RequestFilterSchema.T_ANNO.toString, RequestFilterSchema.T_MESE.toString) &&
          (col(RequestFilterSchema.T_COD_REMI).isNull || consumption(filtroFiledCodRemiConsumption) === col(RequestFilterSchema.T_COD_REMI)) &&
          (col(RequestFilterSchema.T_TRATTAMENTO).isNull || consumption(filtroFiledTrattamentoConsumption) === col(RequestFilterSchema.T_TRATTAMENTO)) &&
          pivaFilter
      )
      .withColumnRenamed(RequestFilterSchema.N_ID_RICHIESTA, idRichiestaFields)
      //      .withColumnRenamed(RequestFilterSchema.D_DATA_RICHIESTA, dataRichiestaFields)
      .withColumnRenamed(RequestFilterSchema.T_PIVA, pivaGestoreFieldConsumption)
    //.selectExpr(fieldsConsumptionRequestRunnable: _*)
  }

  /**
   * Filtra il dataframe dei flussi validati selezionando soltanto i PdR contenuti in [[consumption]].
   * @param consumption dataframe dei consumi filtrati
   * @param validation dataframe dei flussi validati
   * @return dataframe [[validation]] per il solo perimetro di PdR contenuto in [[consumption]].
   */
  def validatedFilter(consumption: DataFrame, validation: DataFrame): DataFrame = {
    val listPdr = consumption.selectExpr(pdrFieldConsumption).distinct

    validation
      .join(listPdr, validation(pdrFieldValidation) === listPdr(pdrFieldConsumption), "inner")
      .drop(listPdr(pdrFieldConsumption))
  }
}
