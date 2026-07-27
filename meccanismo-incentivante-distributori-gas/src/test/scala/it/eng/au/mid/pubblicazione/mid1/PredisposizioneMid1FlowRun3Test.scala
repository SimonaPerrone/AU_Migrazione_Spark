package it.eng.au.mid.pubblicazione.mid1

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao._
import it.eng.au.mid.mock.pubblicazione.PredisposizioneMid1FlowMock
import it.eng.au.mid.model.file.pubblicazione._
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidContatoriModel}
import org.apache.spark.sql.SparkSession
import org.junit.Assert

class PredisposizioneMid1FlowRun3Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Test esecuzione senza file esclusioni, e dailyConsumption avente piu' calcoli per un dato annomese
   */
  def testRun(): Unit = {
    val executionIdDaily = 111L
    val executionIdMidContatori = 123L
    val session = "AGG_FIN"
    val annomese = "202301"
    val pivaDistr = "piva_distr"
    val pivaUdd = "piva_udd"
    val codRemi = "cod_remi"
    val gmd = "gdm"
    val alpha = 35

    val midContatoriDao = new MidContatoriDaoMock(Seq(
      MidContatoriModel(pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "2", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // da scartare: annomese con execution id non massimo
      MidContatoriModel(pdr = "2", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = 100L, executionid_tracciatura = 122L)
    ).toDS())
    val dailyConsumptionAggDao = new DailyConsumptionAggDaoMock(Seq(
      // riga corretta ma con classemisuratore = null, da scartare
      DailyConsumptionModel(pdr = "1", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = null, session= session, executionid = executionIdDaily),
      // da scartare: execution id errato
      DailyConsumptionModel(pdr = "1", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = "g004", session= session, executionid = 100L),
      // execution id corretto
      DailyConsumptionModel(pdr = "2", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      // da scartare: execution id errato
      DailyConsumptionModel(pdr = "2", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = "g004", session= session, executionid = 100L)
    ).toDS())

    val mid1DettaglioDao = new Mid1DettaglioDaoMock(Seq(Mid1DettaglioModel()).toDS())
    val fileEsclusioniPdrDao = new Mid1EsclusioniPdrDaoMock(Seq(MidEsclusioniPdrModel()).toDS())
    val fileEsclusioniTrattamentoDao = new Mid1EsclusioniTrattamentoDaoMock(Seq(MidEsclusioniTrattamentoModel()).toDS())
    val fileEsclusioniAnnomeseDao = new Mid1EsclusioniAnnomeseDaoMock(Seq(MidEsclusioniAnnomeseModel()).toDS())
    val fileEsclusioniDistributoreDao = new Mid1EsclusioniDistributoreDaoMock(Seq(MidEsclusioniDistributoreModel()).toDS())

    val midAlphaValoriDaoMock = new MidAlphaValoriDaoMock(Seq(MidAlphaValoriModel(gdm = gmd, alpha = alpha)).toDS)

    new PredisposizioneMid1FlowMock(
      midContatoriDao = midContatoriDao,
      dailyConsumptionAggDao = dailyConsumptionAggDao,
      mid1DettaglioDao = mid1DettaglioDao,
      fileEsclusioniPdrDao = fileEsclusioniPdrDao,
      fileEsclusioniTrattamentoDao = fileEsclusioniTrattamentoDao,
      fileEsclusioniAnnomeseDao = fileEsclusioniAnnomeseDao,
      fileEsclusioniDistributoreDao = fileEsclusioniDistributoreDao,
      fileAlphaValori = midAlphaValoriDaoMock,
      midAnnomeseDa = annomese,
      midAnnomeseA = annomese,
      sogliaContatore = 1
    ).run()

    val expected = Mid1DettaglioModel(pdr = "2", contatore = 2, piva_id = pivaDistr, piva_udd = pivaUdd, cod_remi = codRemi,
      gdm = gmd, alpha = alpha, executionid_mid_contatori = executionIdMidContatori, annomese = annomese, executionid = Environment.executionId)

    val result = mid1DettaglioDao.ds.cache()

    Assert.assertEquals(1, result.count())
    Assert.assertEquals(expected, result.head())
  }

}
