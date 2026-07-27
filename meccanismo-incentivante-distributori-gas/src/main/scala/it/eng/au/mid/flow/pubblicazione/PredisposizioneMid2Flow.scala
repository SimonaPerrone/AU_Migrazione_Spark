package it.eng.au.mid.flow.pubblicazione

import it.eng.au.mid.common.CostantiMid.{SOC_ATTIVO, SOC_SOSPESO}
import it.eng.au.mid.dao.file.csv.pubblicazione._
import it.eng.au.mid.dao.hive.agg.DailyConsumptionAggDao
import it.eng.au.mid.dao.hive.atg.AtgVariazioniSocDao
import it.eng.au.mid.dao.hive.mid.{Mid2DettaglioDao, MidContatoriDao}
import it.eng.au.mid.dao.hive.rcu.RcuAziendaPDao
import it.eng.au.mid.dao.hive.rcugas.RcugasConnessioniDistr2RemiPDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.Flow
import it.eng.au.mid.model.hive.atg.AtgVariazioniSocModel
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, Mid2DettaglioModel}
import it.eng.au.mid.model.hive.rcu.RcuAziendaPModel
import it.eng.au.mid.model.hive.rcugas.RcugasConnessioniDistr2RemiPModel
import it.eng.au.mid.schema.file.pubblicazione._
import it.eng.au.mid.schema.hive.atg.AtgVariazioniSocSchema
import it.eng.au.mid.schema.hive.mid.{Mid2DettaglioSchema, MidContatoriSchema}
import it.eng.au.mid.schema.hive.rcu.RcuAziendaPSchema
import it.eng.au.mid.schema.hive.rcugas.RcugasConnessioniDistr2RemiPSchema
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{broadcast, col, lit, when}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp
import java.time.LocalDate


/** *
 * Calcola tabella mid2DettaglioDao per periodo compreso tra annoMeseDa e annoMeseA
 */
class PredisposizioneMid2Flow extends Flow {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)
  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  override val flowName: String = "Predisposizione MID2"

  //input
  val midContatoriDao = new MidContatoriDao
  val dailyConsumptionAggDao = new DailyConsumptionAggDao
  val rcuAziendaDao = new RcuAziendaPDao
  val rcugasConnessioniDao = new RcugasConnessioniDistr2RemiPDao
  val atgVariazioniSocDao = new AtgVariazioniSocDao

  //output
  val mid2DettaglioDao = new Mid2DettaglioDao

  // file valori alpha
  val fileAlphaValori = new MidAlphaValoriDao

  // file esclusioni
  val fileEsclusioniPdrDao = new Mid2EsclusioniPdrDao
  val fileEsclusioniTrattamentoDao = new Mid2EsclusioniTrattamentoDao
  val fileEsclusioniAnnomeseDao = new Mid2EsclusioniAnnomeseDao
  val fileEsclusioniDistributoreDao = new Mid2EsclusioniDistributoreDao

  // parametri job
  val midAnnomeseDa: String = Environment.getProperty("job.param.mid2_annomese_da")
  val midAnnomeseA: String = Environment.getProperty("job.param.mid2_annomese_a")
  val sogliaContatore: Int = Environment.getProperty("job.param.mid2_soglia").toInt

  // parametri environment
  val executionId: Long = Environment.executionId
  val dataCalcolo: LocalDate = Environment.processDate

  override def run(): Unit = {
    logger.warn(s"Inizio processo: $flowName")

    logger.warn(s"dataCalcolo: $dataCalcolo")
    logger.warn(s"executionId: $executionId")
    logger.warn(s"midAnnomeseDa: $midAnnomeseDa")
    logger.warn(s"midAnnomeseA: $midAnnomeseA")
    logger.warn(s"sogliaContatore: $sogliaContatore")

    // LETTURA DATI INPUT
    logger.warn(s"Lettura mid contatori: ${midContatoriDao.tableName}")
    val midContatoriDs = midContatoriDao.read()

    logger.warn(s"Lettura dailyConsumption: ${dailyConsumptionAggDao.tableName}")
    val dailyConsumptionAggDs = dailyConsumptionAggDao.read()

    logger.warn(s"Lettura rcuAziendaP: ${rcuAziendaDao.tableName}")
    val ragioneSocialeDs = rcuAziendaDao.read()

    val timestampCalcolo = Timestamp.valueOf(dataCalcolo.atStartOfDay())
    logger.warn(s"Lettura connessioni: ${rcugasConnessioniDao.tableName} al tempo: $timestampCalcolo")
    val connessioniDs = rcugasConnessioniDao.readConnessioniAttive(timestampCalcolo)

    logger.warn(s"Lettura sospensioni società: ${atgVariazioniSocDao.tableName}")
    val sospensioniDs = atgVariazioniSocDao.read()

    // LETTURA FILE ESCLUSIONI
    logger.warn("Lettura file esclusioni")
    logger.warn(s"Esclusioni PDR: ${fileEsclusioniPdrDao.path}")
    val fileEsclusioniPdr = fileEsclusioniPdrDao.read()
      .where(col(MidEsclusioniPdrSchema.pdr).isNotNull)
      .collect().map(_.pdr).toList
    logger.warn(s"${
      if (fileEsclusioniPdr.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniPdr.mkString(", ")
      }
    }")

    logger.warn(s"Esclusioni Trattamento: ${fileEsclusioniTrattamentoDao.path}")
    val fileEsclusioniTrattamento = fileEsclusioniTrattamentoDao.read()
      .where(col(MidEsclusioniTrattamentoSchema.trattamento).isNotNull)
      .collect().map(_.trattamento).toList
    logger.warn(s"${
      if (fileEsclusioniTrattamento.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniTrattamento.mkString(", ")
      }
    }")

    logger.warn(s"Esclusioni Annomese: ${fileEsclusioniAnnomeseDao.path}")
    val fileEsclusioniAnnomese = fileEsclusioniAnnomeseDao.read()
      .where(col(MidEsclusioniAnnomeseSchema.annomese).isNotNull)
      .collect().map(_.annomese).toList
    logger.warn(s"${
      if (fileEsclusioniAnnomese.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniAnnomese.mkString(", ")
      }
    }")

    logger.warn(s"Esclusioni Distributori: ${fileEsclusioniDistributoreDao.path}")
    val fileEsclusioniDistributore = fileEsclusioniDistributoreDao.read()
      .where(col(MidEsclusioniDistributoreSchema.distributore).isNotNull)
      .collect().map(_.distributore).toList
    logger.warn(s"${
      if (fileEsclusioniDistributore.isEmpty) {
        "File vuoto."
      } else {
        fileEsclusioniDistributore.mkString(", ")
      }
    }")

    logger.warn(s"Valori Alpha: ${fileAlphaValori.path}")
    val alphaValoriDs = fileAlphaValori.read()
    logger.warn(alphaValoriDs.show(truncate = false))

    // ELABORAZIONE DATI -> calcolo dati come MID1
    val midContatoriFiltrati = PredisposizioneFunzioni.filtraMidContatoriDaAnalizzare(midContatoriDs, midAnnomeseDa, midAnnomeseA, sogliaContatore)

    val midEsclusiAnnomese = PredisposizioneFunzioni.escludiElementi(midContatoriFiltrati, MidContatoriSchema.annomese, fileEsclusioniAnnomese)
    val midEsclusiPdr = PredisposizioneFunzioni.escludiElementi(midEsclusiAnnomese, MidContatoriSchema.pdr, fileEsclusioniPdr)
    val midEsclusiTrattamenti = PredisposizioneFunzioni.escludiElementi(midEsclusiPdr, MidContatoriSchema.treatment, fileEsclusioniTrattamento)

    val dailyConsumptionAggDsFiltrati = PredisposizioneFunzioni.filtraDailyConsumption(dailyConsumptionAggDs, midEsclusiTrattamenti)

    // ritorna Mid1Dettaglio che verrà successivamente arricchito con le info per mid 2
    val midContatoriArricchiti = PredisposizioneFunzioni.aggiungiInformazioniAnagrafica(midEsclusiTrattamenti, dailyConsumptionAggDsFiltrati, executionId)

    val midEsclusiDistributori = PredisposizioneFunzioni.escludiDistributori(midContatoriArricchiti, fileEsclusioniDistributore)

    val midAlpha = PredisposizioneFunzioni.calcolaAlpha(midEsclusiDistributori, alphaValoriDs)

    // Calcoli specifici MID2 -> da formato MID1 a MID2
    val mid2Dettaglio = convertiModelloMid2(midAlpha)
    val midDistributore = calcolaDistributoreCorrente(mid2Dettaglio, connessioniDs)
    val midAttivoSospeso = calcolaDistributoreAttivoSospeso(midDistributore, sospensioniDs)
    val midDettaglio = aggiungiInformazioniRagioneSociale(midAttivoSospeso, ragioneSocialeDs)

    // SCRITTURA TABELLA FINALE
    logger.warn(s"Scrittura tabella: ${mid2DettaglioDao.tableName}")
    mid2DettaglioDao.write(midDettaglio)

    logger.warn(s"Fine processo: $flowName")
  }

  // FUNZIONI

  /** *
   * Converte a modello MID2 per gli attributi già valorizzati. Il restante a null
   */
  def convertiModelloMid2(mid1Dettaglio: Dataset[Mid1DettaglioModel]): Dataset[Mid2DettaglioModel] = {
    mid1Dettaglio.map(r => Mid2DettaglioModel(
      pdr = r.pdr,
      contatore = r.contatore,
      piva_id = r.piva_id,
      piva_udd = r.piva_udd,
      cod_remi = r.cod_remi,
      gdm = r.gdm,
      alpha = r.alpha,
      executionid_mid_contatori = r.executionid_mid_contatori,
      annomese = r.annomese,
      executionid = r.executionid
    ))
  }

  /** *
   * Calcola distributore attuale su PDR
   */
  def calcolaDistributoreCorrente(midDettaglio: Dataset[Mid2DettaglioModel],
                                  connessioniDs: Dataset[RcugasConnessioniDistr2RemiPModel])
  : Dataset[Mid2DettaglioModel] = {
    // mantieni solo le colonne utili
    val connessioniDsSmall = connessioniDs.select(
      RcugasConnessioniDistr2RemiPSchema.t_codice_pdr,
    RcugasConnessioniDistr2RemiPSchema.t_piva_distr
    )
    midDettaglio.join(
        connessioniDsSmall,
        midDettaglio(Mid2DettaglioSchema.pdr) === connessioniDsSmall(RcugasConnessioniDistr2RemiPSchema.t_codice_pdr),
        "LEFT"
      )
      .withColumn(Mid2DettaglioSchema.piva_distr_att, connessioniDsSmall(RcugasConnessioniDistr2RemiPSchema.t_piva_distr))
      .selectExpr(Mid2DettaglioSchema.getValues: _*)
      .as[Mid2DettaglioModel]
  }

  /** *
   * Calcola se distributore e' "Attivo" o "Sospeso".
   * Se la partita iva del distributore e' presente nella tabella delle cessioni allora va impostato come sospeso, altrimenti attivo
   */
  def calcolaDistributoreAttivoSospeso(midDettaglioDs: Dataset[Mid2DettaglioModel],
                                       sospensioniDs: Dataset[AtgVariazioniSocModel]): Dataset[Mid2DettaglioModel] = {
    midDettaglioDs.join(
        sospensioniDs,
        midDettaglioDs(Mid2DettaglioSchema.piva_id) === sospensioniDs(AtgVariazioniSocSchema.t_piva_distr),
        "LEFT")
      .withColumn(Mid2DettaglioSchema.stato_id,
        when(sospensioniDs(AtgVariazioniSocSchema.t_piva_distr).isNull, lit(SOC_ATTIVO)).otherwise(SOC_SOSPESO))
      .selectExpr(Mid2DettaglioSchema.getValues: _*)
      .as[Mid2DettaglioModel]
  }

  /** *
   * Aggiunge ragione sociale per aziende
   */
  def aggiungiInformazioniRagioneSociale(midDettaglio: Dataset[Mid2DettaglioModel], rcuAzienda: Dataset[RcuAziendaPModel]): Dataset[Mid2DettaglioModel] = {
    val aziendaBroadcast = broadcast(rcuAzienda)
    midDettaglio
      // ragione sociale ID
      .join(
        aziendaBroadcast,
        midDettaglio(Mid2DettaglioSchema.piva_id) === rcuAzienda(RcuAziendaPSchema.t_piva),
        "LEFT")
      .withColumn(Mid2DettaglioSchema.rag_soc_id, rcuAzienda(RcuAziendaPSchema.t_rag_soc))
      .selectExpr(Mid2DettaglioSchema.getValues: _*)
      // ragione sociale UDD
      .join(
        aziendaBroadcast,
        midDettaglio(Mid2DettaglioSchema.piva_udd) === rcuAzienda(RcuAziendaPSchema.t_piva),
        "LEFT"
      )
      .withColumn(Mid2DettaglioSchema.rag_soc_udd, rcuAzienda(RcuAziendaPSchema.t_rag_soc))
      .selectExpr(Mid2DettaglioSchema.getValues: _*)
      // ragione sociale piva att
      .join(
        aziendaBroadcast,
        midDettaglio(Mid2DettaglioSchema.piva_distr_att) === rcuAzienda(RcuAziendaPSchema.t_piva),
        "LEFT"
      )
      .withColumn(Mid2DettaglioSchema.rag_soc_distr_att, rcuAzienda(RcuAziendaPSchema.t_rag_soc))
      .selectExpr(Mid2DettaglioSchema.getValues: _*)
      .as[Mid2DettaglioModel]
  }

}
