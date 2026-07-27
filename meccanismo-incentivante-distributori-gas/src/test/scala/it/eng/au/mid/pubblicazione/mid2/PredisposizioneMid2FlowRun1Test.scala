package it.eng.au.mid.pubblicazione.mid2

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

class PredisposizioneMid2FlowRun1Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Test completi su Mid1, con questo test si verifica l'aggiunta delle ragioni sociali
   */
  def testRun(): Unit = {
    val executionIdDaily = 111L
    val executionIdMidContatori = 123L
    val session = "AGG_S2_FIN"
    val annomese = "202301"
    val pivaDistr = "piva_distr"
    val pivaUdd = "piva_udd"
    val pivaAtt = "piva_att"
    val codRemi = "cod_remi"
    val gmd = "gdm"
    val alpha = 35
    val inizioTs = Timestamp.valueOf("1970-01-01 00:00:00")

    val midContatoriDao = new MidContatoriDaoMock(Seq(
      MidContatoriModel(pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori)
    ).toDS())
    val dailyConsumptionAggDao = new DailyConsumptionAggDaoMock(Seq(
      DailyConsumptionModel(pdr = "1", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      // da ignorare in quanto execution id diverso da daily
      DailyConsumptionModel(pdr = "1", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = "GGG", session = session, executionid = 999L)
    ).toDS())

    val ragioneSocialeDao = new RcuAziendaPDaoMock(Seq(
      RcuAziendaPModel(t_piva = pivaDistr, t_rag_soc = "rag_soc_1"),
      RcuAziendaPModel(t_piva = pivaUdd, t_rag_soc = "rag_soc_2"),
      RcuAziendaPModel(t_piva = pivaAtt, t_rag_soc = "rag_soc_3")
    ).toDS())

    val rcugasConnessioniDao = new RcugasConnessioniDistr2RemiPDaoMock(Seq(
      RcugasConnessioniDistr2RemiPModel(t_codice_pdr = "1", d_data_inizio_conn = inizioTs, d_data_fine_conn = null, d_data_inizio_aggregazione = inizioTs, d_data_fine_aggregazione = null, t_piva_distr = pivaAtt)
    ).toDS()
    )

    val atgVariazioniSocDao = new AtgVariazioniSocDaoMock(Seq(
      AtgVariazioniSocModel(t_piva_distr = pivaDistr)
    ).toDS()
    )

    val midAlphaValoriDaoMock = new MidAlphaValoriDaoMock(Seq(MidAlphaValoriModel(gdm = gmd, alpha = alpha)).toDS)

    val mid2DettaglioDao = new Mid2DettaglioDaoMock(Seq(Mid2DettaglioModel()).toDS())
    val fileEsclusioniPdrDao = new Mid2EsclusioniPdrDaoMock(Seq(MidEsclusioniPdrModel()).toDS())
    val fileEsclusioniTrattamentoDao = new Mid2EsclusioniTrattamentoDaoMock(Seq(MidEsclusioniTrattamentoModel()).toDS())
    val fileEsclusioniAnnomeseDao = new Mid2EsclusioniAnnomeseDaoMock(Seq(MidEsclusioniAnnomeseModel()).toDS())
    val fileEsclusioniDistributoreDao = new Mid2EsclusioniDistributoreDaoMock(Seq(MidEsclusioniDistributoreModel()).toDS())

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
      pdr = "1",
      contatore = 2,
      piva_id = pivaDistr,
      rag_soc_id = "rag_soc_1",
      stato_id = CostantiMid.SOC_SOSPESO,
      piva_udd = pivaUdd,
      rag_soc_udd = "rag_soc_2",
      piva_distr_att = pivaAtt,
      rag_soc_distr_att = "rag_soc_3",
      cod_remi = codRemi,
      gdm = gmd,
      alpha = alpha,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(expected1, result.where(col(Mid1DettaglioSchema.pdr) === "1").head())
  }

}
