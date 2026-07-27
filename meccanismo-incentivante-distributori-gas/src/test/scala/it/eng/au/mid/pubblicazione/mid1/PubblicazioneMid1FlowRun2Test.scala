package it.eng.au.mid.pubblicazione.mid1

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.{Mid1DettaglioDaoMock, MidAggregatoreInfoDaoMock, ZipWriterDaoMock}
import it.eng.au.mid.mock.pubblicazione.PubblicazioneMid1FlowMock
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidAggregatoreInfoModel}
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import java.time.LocalDate

class PubblicazioneMid1FlowRun2Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /**
   * Verifica che i file CSV sono divisi in modo corretto
   */
  def testRun(): Unit = {
    val executionIdMid1Dettaglio = 10L
    val executionId = 1L
    val annoCalcolo = "2024"
    val meseCalcolo = "01"
    val dataCalcolo = LocalDate.parse(s"$annoCalcolo-$meseCalcolo-25")
    val pivaDistr = "piva_distr"
    val sessioneForzata = "AGG_S2"
    val fileTimestamp = "20240125000000"

    // DS con 3 elementi uguali
    val mid1DettaglioDao = new Mid1DettaglioDaoMock(
      ds = (1 to 3).map(_ =>
        Mid1DettaglioModel(
          pdr = "pdr",
          contatore = 2,
          piva_id = pivaDistr,
          piva_udd = "piva_udd",
          cod_remi = "remi",
          gdm = "gdm",
          alpha = 1,
          executionid_mid_contatori = 1L,
          annomese = "202312",
          executionid = executionIdMid1Dettaglio
        )
      ).toDS)
    val zipWriterDao = new ZipWriterDaoMock()
    val midAggregatoreInfoDao = new MidAggregatoreInfoDaoMock(List.empty[MidAggregatoreInfoModel].toDS)

    val pubblicazioneMid1FlowMock = new PubblicazioneMid1FlowMock(
      mid1DettaglioDao = mid1DettaglioDao,
      zipWriterDao = zipWriterDao,
      midAggregatoreInfoDao = midAggregatoreInfoDao,
      executionIdMid1Dettaglio = executionIdMid1Dettaglio,
      sessioneForzata = sessioneForzata,
      percorsoSalvataggio = "/path/salvataggio",
      maxRighePerCsv = 2,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileTimestamp = fileTimestamp
    )
    pubblicazioneMid1FlowMock.run()

    val resultZip = zipWriterDao.ds.cache()

    Assert.assertEquals(2, resultZip.head.files.length)
  }

}
