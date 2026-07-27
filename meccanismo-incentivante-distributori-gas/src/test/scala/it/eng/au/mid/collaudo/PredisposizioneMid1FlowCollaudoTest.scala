package it.eng.au.mid.collaudo

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

class PredisposizioneMid1FlowCollaudoTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Collaudo flusso Predisposizione MID1 (Step 1)
   */
  def testCollaudo(): Unit = {
    val executionIdDaily = 1704280700000L
    val executionIdMidContatori = 1704279600000L
    val annomese = "202401"
    val pivaDistr = "pivadistr1"
    val pivaUdd = "pivaudd1"
    val codRemi = "codremi1"
    val gmd = "G4"
    val trattamento = "Y"
    val session = "AGG_S1_FIN"

    val midContatoriDao = new MidContatoriDaoMock(Seq(
      MidContatoriModel(pdr = "m1_1", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_2", contatore = 1, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_3", contatore = 2, stato = CostantiMid.STATO_INVALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_4", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_5", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = "202402", executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_6", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = "D", annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_7", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_8", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "m1_9", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomese, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori)
    ).toDS())

    val dailyConsumptionAggDao = new DailyConsumptionAggDaoMock(Seq(
      DailyConsumptionModel(pdr = "m1_1", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_2", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_3", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_4", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_5", annomese = "202402", pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_6", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, treatment = "D", session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_7", annomese = annomese, pivadistr = "pivadistr2", pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_8", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = "G16", session = session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "m1_9", annomese = annomese, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = "G4000", session = session, executionid = executionIdDaily)
    ).toDS())

    val mid1DettaglioDao = new Mid1DettaglioDaoMock(Seq(Mid1DettaglioModel()).toDS())

    val fileEsclusioniPdrDao = new Mid1EsclusioniPdrDaoMock(Seq(
      MidEsclusioniPdrModel(pdr = "m1_4")
    ).toDS())
    val fileEsclusioniTrattamentoDao = new Mid1EsclusioniTrattamentoDaoMock(Seq(
      MidEsclusioniTrattamentoModel(trattamento = "D")
    ).toDS())
    val fileEsclusioniAnnomeseDao = new Mid1EsclusioniAnnomeseDaoMock(Seq(
      MidEsclusioniAnnomeseModel(annomese = "202402")
    ).toDS())
    val fileEsclusioniDistributoreDao = new Mid1EsclusioniDistributoreDaoMock(Seq(
      MidEsclusioniDistributoreModel(distributore = "pivadistr2")
    ).toDS())

    val midAlphaValoriDaoMock = new MidAlphaValoriDaoMock(Seq(
      MidAlphaValoriModel(gdm = gmd, alpha = 35),
      MidAlphaValoriModel(gdm = "G16", alpha =70),
      MidAlphaValoriModel(gdm = "G4000", alpha = 140)
    ).toDS)

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
      pdr = "m1_1",
      contatore = 2,
      piva_id = pivaDistr,
      piva_udd = pivaUdd,
      cod_remi = codRemi,
      gdm = gmd,
      alpha = 35,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )
    val expected8 = Mid1DettaglioModel(
      pdr = "m1_8",
      contatore = 2,
      piva_id = pivaDistr,
      piva_udd = pivaUdd,
      cod_remi = codRemi,
      gdm = "G16",
      alpha = 70,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )
    val expected9 = Mid1DettaglioModel(
      pdr = "m1_9",
      contatore = 2,
      piva_id = pivaDistr,
      piva_udd = pivaUdd,
      cod_remi = codRemi,
      gdm = "G4000",
      alpha = 140,
      executionid_mid_contatori = executionIdMidContatori,
      annomese = annomese,
      executionid = Environment.executionId
    )

    Assert.assertEquals(3, result.count())
    Assert.assertEquals(expected1, result.where(col(Mid1DettaglioSchema.pdr) === "m1_1").head())
    Assert.assertEquals(expected8, result.where(col(Mid1DettaglioSchema.pdr) === "m1_8").head())
    Assert.assertEquals(expected9, result.where(col(Mid1DettaglioSchema.pdr) === "m1_9").head())
  }

}
