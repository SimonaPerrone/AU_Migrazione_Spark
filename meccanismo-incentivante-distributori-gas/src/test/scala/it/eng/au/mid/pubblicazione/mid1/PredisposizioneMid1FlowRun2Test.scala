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

class PredisposizioneMid1FlowRun2Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Test esecuzione con file di esclusione valorizzati
   */
  def testRun(): Unit = {
    val executionIdDaily = 111L
    val executionIdMidContatori = 123L
    val session = "AGG_PRE"
    val annomeseDa = "202301"
    val annomeseA = "202303"
    val pivaDistr = "piva_distr"
    val pivaUdd = "piva_udd"
    val codRemi = "cod_remi"
    val gmd = "gdm"
    val trattamento = "Y"

    val midContatoriDao = new MidContatoriDaoMock(Seq(
      // OK
      MidContatoriModel(pdr = "1", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomeseDa, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      MidContatoriModel(pdr = "2", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomeseA, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso annomese
      MidContatoriModel(pdr = "3", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = "202302", executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso pdr
      MidContatoriModel(pdr = "4", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomeseA, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso trattamento
      MidContatoriModel(pdr = "5", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = "G", annomese = annomeseA, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori),
      // escluso piva distr
      MidContatoriModel(pdr = "6", contatore = 2, stato = CostantiMid.STATO_VALIDO, treatment = trattamento, annomese = annomeseA, executionid_daily_consumption = executionIdDaily, executionid_tracciatura = executionIdMidContatori)
    ).toDS())
    val dailyConsumptionAggDao = new DailyConsumptionAggDaoMock(Seq(
      DailyConsumptionModel(pdr = "1", annomese = annomeseDa, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "2", annomese = annomeseA, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "3", annomese = "202302", pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "4", annomese = annomeseA, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "5", annomese = annomeseA, pivadistr = pivaDistr, pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily),
      DailyConsumptionModel(pdr = "6", annomese = annomeseA, pivadistr = "d1", pivaudd = pivaUdd, codremi = codRemi, classemisuratore = gmd, session= session, executionid = executionIdDaily)
    ).toDS())
    val fileEsclusioniPdrDao = new Mid1EsclusioniPdrDaoMock(Seq(
      MidEsclusioniPdrModel(pdr = "4")
    ).toDS())
    val fileEsclusioniTrattamentoDao = new Mid1EsclusioniTrattamentoDaoMock(Seq(
      MidEsclusioniTrattamentoModel(trattamento = "G")
    ).toDS())
    val fileEsclusioniAnnomeseDao = new Mid1EsclusioniAnnomeseDaoMock(Seq(
      MidEsclusioniAnnomeseModel(annomese = "202302")
    ).toDS())
    val fileEsclusioniDistributoreDao = new Mid1EsclusioniDistributoreDaoMock(Seq(
      MidEsclusioniDistributoreModel(distributore = "d1")
    ).toDS())
    val mid1DettaglioDao = new Mid1DettaglioDaoMock(Seq(Mid1DettaglioModel()).toDS())

    val midAlphaValoriDaoMock = new MidAlphaValoriDaoMock(Seq(MidAlphaValoriModel(gdm = gmd, alpha = 35)).toDS)

    new PredisposizioneMid1FlowMock(
      midContatoriDao = midContatoriDao,
      dailyConsumptionAggDao = dailyConsumptionAggDao,
      mid1DettaglioDao = mid1DettaglioDao,
      fileEsclusioniPdrDao = fileEsclusioniPdrDao,
      fileEsclusioniTrattamentoDao = fileEsclusioniTrattamentoDao,
      fileEsclusioniAnnomeseDao = fileEsclusioniAnnomeseDao,
      fileEsclusioniDistributoreDao = fileEsclusioniDistributoreDao,
      fileAlphaValori = midAlphaValoriDaoMock,
      midAnnomeseDa = annomeseDa,
      midAnnomeseA = annomeseA,
      sogliaContatore = 1
    ).run()

    val result = mid1DettaglioDao.ds.cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(1, result.where(col(Mid1DettaglioSchema.pdr) === "1").count())
    Assert.assertEquals(1, result.where(col(Mid1DettaglioSchema.pdr) === "2").count())
  }

}
