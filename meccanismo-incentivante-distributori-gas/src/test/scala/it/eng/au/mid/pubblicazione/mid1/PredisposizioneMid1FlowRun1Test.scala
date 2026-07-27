package it.eng.au.mid.pubblicazione.mid1

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao._
import it.eng.au.mid.mock.pubblicazione.PredisposizioneMid1FlowMock
import it.eng.au.mid.model.file.pubblicazione._
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidContatoriModel}
import it.eng.au.mid.schema.hive.mid.Mid1DettaglioSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

class PredisposizioneMid1FlowRun1Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Test esecuzione senza file esclusioni
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
      MidContatoriModel(pdr = "2", contatore = 2, stato = CostantiMid.STATO_FORZATO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // stato invalido
      MidContatoriModel(pdr = "3", contatore = 2, stato = CostantiMid.STATO_INVALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // pdr senza piva distr
      MidContatoriModel(pdr = "4", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // pdr senza piva udd
      MidContatoriModel(pdr = "5", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // pdr senza cod remi
      MidContatoriModel(pdr = "6", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // pdr senza cod gdm
      MidContatoriModel(pdr = "7", contatore = 2, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // contatore inferiore-uguale a soglia
      MidContatoriModel(pdr = "8", contatore = 1, stato = CostantiMid.STATO_VALIDO, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori)
    ).toDS())
    val dailyConsumptionAggDao = new DailyConsumptionAggDaoMock(Seq(
      DailyConsumptionModel(pdr = "1", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "2", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "3", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "4", annomese = annomese, pivadistr = null, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "5", annomese = annomese, pivadistr = pivaDistr, pivaudd = null, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "6", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = null, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "7", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = null, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "8", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily)
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

    val result = mid1DettaglioDao.ds.cache()

    val expected1 = Mid1DettaglioModel(
      pdr = "1",
      contatore = 2,
      piva_id = pivaDistr,
      piva_udd = pivaUdd,
      cod_remi = codRemi,
      gdm = gmd,
      alpha = alpha,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )
    val expected2 = Mid1DettaglioModel(
      pdr = "2",
      contatore = 2,
      piva_id = pivaDistr,
      piva_udd = pivaUdd,
      cod_remi = codRemi,
      gdm = gmd,
      alpha = alpha,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(expected1, result.where(col(Mid1DettaglioSchema.pdr) === "1").head())
    Assert.assertEquals(expected2, result.where(col(Mid1DettaglioSchema.pdr) === "2").head())
  }

}
