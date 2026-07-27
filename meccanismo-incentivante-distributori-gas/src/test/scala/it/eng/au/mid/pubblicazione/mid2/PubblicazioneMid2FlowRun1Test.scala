package it.eng.au.mid.pubblicazione.mid2

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.{Mid2DettaglioDaoMock, MidAggregatoreInfoDaoMock, ZipWriterDaoMock}
import it.eng.au.mid.mock.pubblicazione.PubblicazioneMid2FlowMock
import it.eng.au.mid.model.file.pubblicazione.{FileModel, ZipWriterModel}
import it.eng.au.mid.model.hive.mid.{Mid2DettaglioModel, MidAggregatoreInfoModel}
import it.eng.au.mid.schema.file.pubblicazione.Mid2Schema
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import java.sql.Date
import java.time.LocalDate

class PubblicazioneMid2FlowRun1Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

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

    val mid2DettaglioDao = new Mid2DettaglioDaoMock(ds = Seq(
      Mid2DettaglioModel(
        pdr = "pdr",
        contatore = 2,
        piva_id = pivaDistr,
        rag_soc_id = "rag_soc_id",
        piva_udd = "piva_udd",
        rag_soc_udd = "rag_soc_udd",
        cod_remi = "remi",
        gdm = "gdm",
        alpha = 1,
        executionid_mid_contatori = 1L,
        annomese = "202312",
        executionid = executionIdMid2Dettaglio
      )
    ).toDS)
    val zipWriterDao = new ZipWriterDaoMock()
    val midAggregatoreInfoDao = new MidAggregatoreInfoDaoMock(List.empty[MidAggregatoreInfoModel].toDS)

    val pubblicazioneMid2FlowMock = new PubblicazioneMid2FlowMock(
      mid2DettaglioDao = mid2DettaglioDao,
      zipWriterDao = zipWriterDao,
      midAggregatoreInfoDao = midAggregatoreInfoDao,
      executionIdMid2Dettaglio = executionIdMid2Dettaglio,
      sessioneForzata = sessioneForzata,
      percorsoSalvataggio = "/path/salvataggio",
      maxRighePerCsv = 10,
      cseaPartitaIva = cseaPartitaIva,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileTimestamp = fileTimestamp
    )
    pubblicazioneMid2FlowMock.run()

    val csvContenuto = {
      val intestazione = s"${Mid2Schema.getValues.mkString(pubblicazioneMid2FlowMock.csvSeparatore)}\n"
      val contenuto = Seq(pivaDistr, "rag_soc_id", "", "piva_udd", "rag_soc_udd", "pdr", "202312", 2, "remi", "gdm", 1, "", "").mkString(pubblicazioneMid2FlowMock.csvSeparatore)
      intestazione + contenuto
    }

    val expectedZip = ZipWriterModel(
      pivaId = pivaDistr,
      fileName = s"/path/salvataggio/$annoCalcolo/$meseCalcolo/${pivaDistr}_${CostantiMid.NOME_OPERAZIONE_MID2}_${sessioneForzata}_${fileTimestamp}_1.zip",
      files = List(
        FileModel(
          fileName = s"${pivaDistr}_MID2_${sessioneForzata}_${fileTimestamp}_1.csv",
          content = csvContenuto.getBytes()
        ))
    )

    val expectedMidAggregatoreInfo = MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID2,
      nome_file = s"${pivaDistr}_${CostantiMid.NOME_OPERAZIONE_MID2}_${sessioneForzata}_${fileTimestamp}_1.zip",
      path = s"/path/salvataggio/$annoCalcolo/$meseCalcolo/${pivaDistr}_${CostantiMid.NOME_OPERAZIONE_MID2}_${sessioneForzata}_${fileTimestamp}_1.zip",
      tipo_dest = CostantiMid.DESTINAZIONE_MID2,
      piva_dest = cseaPartitaIva,
      piva_id_file = pivaDistr,
      piva_udd_file = null,
      data_caricamento = Date.valueOf(dataCalcolo),
      executionid_mid_dettaglio = executionIdMid2Dettaglio,
      executionid = executionId
    )

    val resultZip = zipWriterDao.ds.head()

    Assert.assertEquals(expectedZip, resultZip)
    Assert.assertEquals(expectedMidAggregatoreInfo, midAggregatoreInfoDao.ds.head())
  }

}
