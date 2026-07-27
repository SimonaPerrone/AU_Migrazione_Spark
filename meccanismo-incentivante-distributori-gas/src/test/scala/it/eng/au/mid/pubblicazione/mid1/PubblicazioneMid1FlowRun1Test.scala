package it.eng.au.mid.pubblicazione.mid1

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.{Mid1DettaglioDaoMock, MidAggregatoreInfoDaoMock, ZipWriterDaoMock}
import it.eng.au.mid.mock.pubblicazione.PubblicazioneMid1FlowMock
import it.eng.au.mid.model.file.pubblicazione.{FileModel, ZipWriterModel}
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidAggregatoreInfoModel}
import it.eng.au.mid.schema.file.pubblicazione.Mid1Schema
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import java.sql.Date
import java.time.LocalDate

class PubblicazioneMid1FlowRun1Test extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testRun(): Unit = {
    val executionIdMid1Dettaglio = 10L
    val executionId = 1L
    val annoCalcolo = "2024"
    val meseCalcolo = "01"
    val dataCalcolo = LocalDate.parse(s"$annoCalcolo-$meseCalcolo-25")
    val pivaDistr = "piva_distr"
    val sessioneForzata = "AGG_S2"
    val fileTimestamp = "20240125000000"

    val mid1DettaglioDao = new Mid1DettaglioDaoMock(ds = Seq(
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
      maxRighePerCsv = 10,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileTimestamp = fileTimestamp
    )
    pubblicazioneMid1FlowMock.run()


    //  PIVA_DISTR, PIVA_UDD,PDR,ANNOMESE,N,COD_REMI,GDM, ALPHA
    val csvContenuto = {
      val intestazione = s"${Mid1Schema.getValues.mkString(pubblicazioneMid1FlowMock.csvSeparatore)}\n"
      val contenuto = Seq(pivaDistr, "piva_udd", "pdr", "202312", 2, "remi", "gdm", 1).mkString(pubblicazioneMid1FlowMock.csvSeparatore)
      intestazione + contenuto
    }

    val expectedZip = ZipWriterModel(
      pivaId = pivaDistr,
      fileName = s"/path/salvataggio/AGG4_$pivaDistr/$annoCalcolo/$meseCalcolo/${pivaDistr}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      files = List(
        FileModel(
          fileName = s"${pivaDistr}_piva_udd_MID1_${sessioneForzata}_${fileTimestamp}_1.csv",
          content = csvContenuto.getBytes()
        ))
    )

    val expectedMidAggregatoreInfo = MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID1,
      nome_file = s"${pivaDistr}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      path = s"/path/salvataggio/AGG4_$pivaDistr/$annoCalcolo/$meseCalcolo/${pivaDistr}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      tipo_dest = CostantiMid.DESTINAZIONE_MID1,
      piva_dest = pivaDistr,
      piva_id_file = pivaDistr,
      piva_udd_file = null,
      data_caricamento = Date.valueOf(dataCalcolo),
      executionid_mid_dettaglio = executionIdMid1Dettaglio,
      executionid = executionId
    )

    val resultZip = zipWriterDao.ds.head()

    Assert.assertEquals(expectedZip, resultZip)
    Assert.assertEquals(expectedMidAggregatoreInfo, midAggregatoreInfoDao.ds.head())
  }

}
