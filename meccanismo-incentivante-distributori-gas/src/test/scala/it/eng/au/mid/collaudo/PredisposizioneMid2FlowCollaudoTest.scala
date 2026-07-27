package it.eng.au.mid.collaudo

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.dao.hive.atg.AtgVariazioniSocDao
import it.eng.au.mid.dao.hive.rcugas.RcugasConnessioniDistr2RemiPDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao._
import it.eng.au.mid.mock.pubblicazione.PredisposizioneMid2FlowMock
import it.eng.au.mid.model.file.pubblicazione._
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.hive.atg.AtgVariazioniSocModel
import it.eng.au.mid.model.hive.mid.{Mid2DettaglioModel, MidContatoriModel}
import it.eng.au.mid.model.hive.rcu.RcuAziendaPModel
import it.eng.au.mid.model.hive.rcugas.RcugasConnessioniDistr2RemiPModel
import it.eng.au.mid.schema.hive.mid.Mid1DettaglioSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Timestamp

class PredisposizioneMid2FlowCollaudoTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Collaudo flusso Predisposizione MID2 (Step 1)
   */
  def testCollaudo(): Unit = {
    val executionIdDaily = 1704280700000L
    val executionIdMidContatori = 1704279600000L
    val annomese = "202401"
    val pivaDistr = "pivadistr1" // piva distr con stato Sospeso
    val pivaDistrAtt = "pivadistrAtt1" // piva distr con stato Attivo
    val pivaUdd = "pivaudd1"
    val pivaAtt = "piva_att"
    val codRemi = "codremi1"
    val gmd = "G4"
    val trattamento = "Y"
    val ragSocialeDistr = "ragsocdistr1"
    val ragSocialeUdd = "ragsocudd1"
    val ragSocialeAtt = "ragsocuatt1"
    val session = "AGG_S1_FIN"
    val inizioTs = Timestamp.valueOf("1970-01-01 00:00:00")

    val midContatoriDao = new MidContatoriDaoMock(Seq(
      // ok
      MidContatoriModel(pdr = "m2_1", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // soglia contatore inferiore a richiesto
      MidContatoriModel(pdr = "m2_2", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // stato invalido
      MidContatoriModel(pdr = "m2_3", contatore = 2, stato = CostantiMid.STATO_INVALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso da file per pdr
      MidContatoriModel(pdr = "m2_4", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso per annomese fuori perimetro
      MidContatoriModel(pdr = "m2_5", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = "202402", executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso da file per trattamento
      MidContatoriModel(pdr = "m2_6", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = "D", annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso da file per piva distr
      MidContatoriModel(pdr = "m2_7", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // ok
      MidContatoriModel(pdr = "m2_8", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // ok
      MidContatoriModel(pdr = "m2_9", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori)
    ).toDS())

    val dailyConsumptionAggDao = new DailyConsumptionAggDaoMock(Seq(
      DailyConsumptionModel(pdr = "m2_1", annomese = annomese, pivadistr = pivaDistrAtt, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_2", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_3", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_4", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_5", annomese = "202402", pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_6", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, treatment = "D", session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_7", annomese = annomese, pivadistr = "pivadistr2", pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_8", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = "G16", session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m2_9", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = "G4000", session = session, executionid = executionIdDaily)
    ).toDS())

    val fileEsclusioniPdrDao = new Mid2EsclusioniPdrDaoMock(Seq(
      MidEsclusioniPdrModel(pdr = "m2_4")
    ).toDS())

    val fileEsclusioniTrattamentoDao = new Mid2EsclusioniTrattamentoDaoMock(Seq(
      MidEsclusioniTrattamentoModel(trattamento = "D")
    ).toDS())

    val fileEsclusioniAnnomeseDao = new Mid2EsclusioniAnnomeseDaoMock(Seq(
      MidEsclusioniAnnomeseModel(annomese = "202402")
    ).toDS())

    val fileEsclusioniDistributoreDao = new Mid2EsclusioniDistributoreDaoMock(Seq(
      MidEsclusioniDistributoreModel(distributore = "pivadistr2")
    ).toDS())

    val midAlphaValoriDaoMock = new MidAlphaValoriDaoMock(Seq(
      MidAlphaValoriModel(gdm = gmd, alpha = 35),
      MidAlphaValoriModel(gdm = "G16", alpha = 70),
      MidAlphaValoriModel(gdm = "G4000", alpha = 140)
    ).toDS)

    val ragioneSocialeDao = new RcuAziendaPDaoMock(Seq(
      RcuAziendaPModel(t_piva = pivaDistr, t_rag_soc = ragSocialeDistr),
      RcuAziendaPModel(t_piva = pivaDistrAtt, t_rag_soc = ragSocialeDistr),
      RcuAziendaPModel(t_piva = pivaUdd, t_rag_soc = ragSocialeUdd),
      RcuAziendaPModel(t_piva = pivaAtt, t_rag_soc = ragSocialeAtt)
    ).toDS())

    val rcugasConnessioniDao = new RcugasConnessioniDistr2RemiPDaoMock(Seq(
      RcugasConnessioniDistr2RemiPModel(t_codice_pdr = "m2_1", d_data_inizio_conn = inizioTs, d_data_fine_conn = null, d_data_inizio_aggregazione = inizioTs, d_data_fine_aggregazione = null, t_piva_distr = pivaAtt)
    ).toDS()
    )

    val atgVariazioniSocDao = new AtgVariazioniSocDaoMock(Seq(
      AtgVariazioniSocModel(t_piva_distr = pivaDistr)
    ).toDS()
    )

    val mid2DettaglioDao = new Mid2DettaglioDaoMock(Seq(Mid2DettaglioModel()).toDS())


    new PredisposizioneMid2FlowMock(
      midContatoriDao = midContatoriDao,
      dailyConsumptionAggDao = dailyConsumptionAggDao,
      mid2DettaglioDao = mid2DettaglioDao,
      rcuAziendaDao = ragioneSocialeDao,
      rcugasConnessioniDao: RcugasConnessioniDistr2RemiPDao,
      atgVariazioniSocDao: AtgVariazioniSocDao,
      fileEsclusioniPdrDao = fileEsclusioniPdrDao,
      fileEsclusioniTrattamentoDao = fileEsclusioniTrattamentoDao,
      fileEsclusioniAnnomeseDao = fileEsclusioniAnnomeseDao,
      fileEsclusioniDistributoreDao = fileEsclusioniDistributoreDao,
      fileAlphaValori = midAlphaValoriDaoMock,
      midAnnomeseDa = annomese,
      midAnnomeseA = annomese,
      sogliaContatore = 1
    ).run()

    val result = mid2DettaglioDao.ds.cache()

    val expected1 = Mid2DettaglioModel(
      pdr = "m2_1",
      contatore = 2,
      piva_id = pivaDistrAtt,
      rag_soc_id = ragSocialeDistr,
      stato_id = CostantiMid.SOC_ATTIVO,
      piva_udd = pivaUdd,
      rag_soc_udd = ragSocialeUdd,
      piva_distr_att = pivaAtt,
      rag_soc_distr_att = ragSocialeAtt,
      cod_remi = codRemi,
      gdm = gmd,
      alpha = 35,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )
    val expected8 = Mid2DettaglioModel(
      pdr = "m2_8",
      contatore = 2,
      piva_id = pivaDistr,
      rag_soc_id = ragSocialeDistr,
      stato_id = CostantiMid.SOC_SOSPESO,
      piva_udd = pivaUdd,
      rag_soc_udd = ragSocialeUdd,
      cod_remi = codRemi,
      gdm = "G16",
      alpha = 70,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )
    val expected9 = Mid2DettaglioModel(
      pdr = "m2_9",
      contatore = 2,
      piva_id = pivaDistr,
      rag_soc_id = ragSocialeDistr,
      stato_id = CostantiMid.SOC_SOSPESO,
      piva_udd = pivaUdd,
      rag_soc_udd = ragSocialeUdd,
      cod_remi = codRemi,
      gdm = "G4000",
      alpha = 140,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(expected1, result.where(col(Mid1DettaglioSchema.pdr) === "m2_1").head())
    Assert.assertEquals(expected8, result.where(col(Mid1DettaglioSchema.pdr) === "m2_8").head())
    Assert.assertEquals(expected9, result.where(col(Mid1DettaglioSchema.pdr) === "m2_9").head())
  }

}
