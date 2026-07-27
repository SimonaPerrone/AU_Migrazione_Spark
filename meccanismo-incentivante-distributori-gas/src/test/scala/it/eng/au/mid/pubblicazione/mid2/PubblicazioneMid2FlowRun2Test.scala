package it.eng.au.mid.pubblicazione.mid2

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.{Mid2DettaglioDaoMock, MidAggregatoreInfoDaoMock, ZipWriterDaoMock}
import it.eng.au.mid.mock.pubblicazione.PubblicazioneMid2FlowMock
import it.eng.au.mid.model.hive.mid.{Mid2DettaglioModel, MidAggregatoreInfoModel}
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import java.time.LocalDate

class PubblicazioneMid2FlowRun2Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /**
   * Verifica che i file CSV sono divisi in modo corretto
   */
  def testRun(): Unit = {
    val executionIdMid2Dettaglio = 10L
    val executionId = 1L
    val annoCalcolo = "2024"
    val meseCalcolo = "01"
    val dataCalcolo = LocalDate.parse(s"$annoCalcolo-$meseCalcolo-25")
    val pivaDistr = "piva_distr"
    val sessioneForzata = "AGG_S2"
    val cseaPartitaIva = "pivaCsea"
    val fileTimestamp = "20240125000000"

    val mid2DettaglioDao = new Mid2DettaglioDaoMock(
      // 3 elementi uguali
      ds = (1 to 3)
        .map(_ => Mid2DettaglioModel(
          pdr = "pdr",
          contatore = 2,
          piva_id = pivaDistr,
          rag_soc_id = "rag_soc_id",
          piva_udd = "piva_udd",
          rag_soc_udd = "rag_soc_udd1",
          cod_remi = "remi",
          gdm = "gdm",
          alpha = 1,
          executionid_mid_contatori = 1L,
          annomese = "202312",
          executionid = executionIdMid2Dettaglio
        )).toDS()
    )
    val zipWriterDao = new ZipWriterDaoMock()
    val midAggregatoreInfoDao = new MidAggregatoreInfoDaoMock(List.empty[MidAggregatoreInfoModel].toDS)

    val pubblicazioneMid2FlowMock = new PubblicazioneMid2FlowMock(
      mid2DettaglioDao = mid2DettaglioDao,
      zipWriterDao = zipWriterDao,
      midAggregatoreInfoDao = midAggregatoreInfoDao,
      executionIdMid2Dettaglio = executionIdMid2Dettaglio,
      sessioneForzata = sessioneForzata,
      percorsoSalvataggio = "/path/salvataggio",
      maxRighePerCsv = 2,
      cseaPartitaIva = cseaPartitaIva,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileTimestamp = fileTimestamp
    )
    pubblicazioneMid2FlowMock.run()

    val resultZip = zipWriterDao.ds.cache()

    Assert.assertEquals(2, resultZip.head.files.length)
  }

}
