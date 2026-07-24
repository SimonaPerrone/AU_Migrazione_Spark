package it.eng.au.calcoloIndennizzi

import it.eng.au.calcoloIndennizzi.controller._
import it.eng.au.calcoloIndennizzi.dao.measure.TglDAO
import it.eng.au.calcoloIndennizzi.dao.output._
import it.eng.au.calcoloIndennizzi.dao.rcu.RcuAziendaDAO
import it.eng.au.calcoloIndennizzi.dao.rcugas._
import it.eng.au.calcoloIndennizzi.utility.args.ArgsFactory
import it.eng.au.calcoloIndennizzi.utility.{CalcoloEnvironment, Properties}
import it.eng.au.indennizziMisureGasCommon.utility.log.LogUtility
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = ArgsFactory.parse(args)

      CalcoloEnvironment.setEnvironment(parsedArgs)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()
    } catch {
      case e: Throwable =>
        logger.error(s"An error occurred in the procedure.")
        throw e
    }
  }

  def run(): Unit = {
    val rcugasMassivoDAO = new RcugasMassivoPDAO
    val rcugasVarMisuratoreDAO = new RcugasVarMisuratoreDAO
    val rcugasConnessioniDistr2DAO = new RcugasConnessioniDistr2DAO
    val rcugasSospensioniDAO = new RcugasSospensioniPDAO
    val rcugasVarTrattamentoDAO = new RcugasVarTrattamentoPDAO
    val rcuAziendaDAO = new RcuAziendaDAO
    val tglDAO = new TglDAO
    val aggregatoTotaleDAO = new AggregatoTotaleDAO
    val pdrTotaleDAO = new PdrTotaleDAO
    val dettaglioPdrDAO = new DettaglioPdrDAO
    val dettaglioOM1DAO = new DettaglioOM1DAO
    val dettaglioOM2DAO = new DettaglioOM2DAO
    val dettaglioOM3DAO = new DettaglioOM3DAO

    // Lettura della massivo
    // Filtro dei pdr attivi nel mese M (e con un unico UdD all'interno del mese)
    val rcugasMassivo = rcugasMassivoDAO.get

    // Lettura della sospensioni
    val rcugasSospensioni = rcugasSospensioniDAO.get

    // Lettura della var_misuratore
    val rcugasVarMisuratore = rcugasVarMisuratoreDAO.get

    // Lettura della rcu_azienda (per i campi rag_soc_udd e rag_soc_distr)
    val rcuAziendaForId = rcuAziendaDAO.get
    val rcuAziendaForUdd = rcuAziendaDAO.get

    // Lettura dei filtri di esclusione PdR
    // Rimozione di eventuali PdR dalla massivo
    val rcugasMassivoFiltered = ExclusionFilterController.excludePdrs(rcugasMassivo)

    // Lettura della trattamento
    // Filtro dei pdr con trattamento G in ogni giorno del mese M
    val rcugasVarTrattamento = rcugasVarTrattamentoDAO.get

    // Lettura della connessioni_distr2
    // Filtro dei pdr con un unico Distr all'interno del mese M
    val rcugasConnessioniDistr2 = rcugasConnessioniDistr2DAO.get

    // Dataframe pdrG che identifica il perimetro pdr di base, ovvero tutti i pdr per cui
    // - il pdr è attivo nel mese M,
    // - il pdr non è sospeso nel mese M,
    // - il pdr ha trattamento G in tutti i giorni del mese M,
    // - il pdr ha misuratore>=G10 in tutti i giorni del mese M,
    // - il pdr è associato a una coppia (pivaudd, pivadistr) che gestisce un numero di pdr > 10.
    val pdrG = PdrGController.getPdrGDataFrame(rcugasMassivoFiltered, rcugasVarMisuratore, rcugasVarTrattamento, rcugasSospensioni, rcugasConnessioniDistr2)

    // Left join tra pdrG e rcuAzienda per ottenere i campi rag_soc_udd e rag_soc_distr
    val pdrGWithRagSoc = PdrGController.getRagioneSociale(pdrG, rcuAziendaForId, rcuAziendaForUdd)

    // Se recovery.mode è true, selezioniamo solo una parte dei pdrG
    val pdrGFiltered = if (Properties.isRecoveryMode) RecoveryController.filter(pdrGWithRagSoc) else pdrGWithRagSoc

    // Lettura della tabella contenente le TGL
    // Filtro delle TGL ammissibili
    // Rimozione delle misure duplicate
    // Esclusione delle TGL da file
    // Applicazione della priorità per selezionare un'unica TGL per ogni giorno
    val tgl = TglController.getTgl(tglDAO.readTable)

    // Aggregazione delle Tgl per PdR in modo da calcolare le count delle letture effettive/stimate
    val tglAggregated = TglController.aggregateTgl(tgl)

    // Calcolo del numero di tipo_lettura="E" all'interno delle tgl in modo tale da sapere se la tgl concorre al calcolo delle regole 2 e 3
    val tglWithInfo = TglController.getInfo(tglAggregated)

    // Left join tra pdrG e le TGL
    val pdrGSettimo = PdrGController.getPdrGSettimoDataFrame(pdrGFiltered, tglWithInfo)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // Calcoliamo il numero di PDR_G, PDR_G_OM1, PDR_G_OM2, PDR_G_OM3 per ogni coppia (pivadistr, pivaudd)
    val pdrCount = PdrGController.getPdRCount(pdrGSettimo)

    // Calcolo degli indennizzi
    // Creazione della tabella Aggregato Totale
    // Forzatura a null dei campi relativi a una regola se tale regola è disattivata
    // Scrittura della tabella Aggregato Totale
    val aggregatoTotale = IndennizziController.calcoloIndennizzi(pdrCount)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    val aggregatoTotaleForced = IndennizziController.forceNulls(aggregatoTotale)
    aggregatoTotaleDAO.write(aggregatoTotaleForced)

    // Creazione e scrittura delle tabelle di dettaglio OM1, OM2 e OM3
    val dettaglioOM1 = dettaglioOM1DAO.get(aggregatoTotale)
    dettaglioOM1DAO.write(dettaglioOM1)
    val dettaglioOM2 = dettaglioOM2DAO.get(aggregatoTotale)
    dettaglioOM2DAO.write(dettaglioOM2)
    val dettaglioOM3 = dettaglioOM3DAO.get(aggregatoTotale)
    dettaglioOM3DAO.write(dettaglioOM3)

    // Creazione della tabella Pdr Totale
    // Forzatura a null dei campi relativi a una regola se tale regola è disattivata
    // Scrittura della tabella Pdr Totale
    val pdrTotale = PdrGController.getPdrTotale(pdrGSettimo, aggregatoTotale)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val pdrTotaleForced = PdrGController.forceNulls(pdrTotale)
    pdrTotaleDAO.write(pdrTotaleForced)

    aggregatoTotale.unpersist

    // Creazione e scrittura della tabella Dettaglio Pdr
    val dettaglioPdr = PdrGController.getDettaglioPdr(pdrTotale)
    dettaglioPdrDAO.write(dettaglioPdr)
  }
}