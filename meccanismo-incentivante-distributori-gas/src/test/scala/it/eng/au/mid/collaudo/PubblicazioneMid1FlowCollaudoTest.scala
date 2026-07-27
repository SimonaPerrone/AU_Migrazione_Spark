package it.eng.au.mid.collaudo

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.{Mid1DettaglioDaoMock, MidAggregatoreInfoDaoMock, ZipWriterDaoMock}
import it.eng.au.mid.mock.pubblicazione.PubblicazioneMid1FlowMock
import it.eng.au.mid.model.file.pubblicazione.{FileModel, Mid1Model, ZipWriterModel}
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidAggregatoreInfoModel}
import it.eng.au.mid.schema.file.pubblicazione.ZipCsvSchema
import it.eng.au.mid.schema.hive.mid.MidAggregatoreInfoSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Date
import java.time.LocalDate

class PubblicazioneMid1FlowCollaudoTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Collaudo flusso Pubblicazione MID1 (Step 2)
   */
  def testCollaudo(): Unit = {
    val executionIdMid1Dettaglio = 1704279600000L
    val executionId = 1704280700000L
    val sessioneForzata = "AGG_FORZ"
    val annoCalcolo = "2024"
    val meseCalcolo = "02"
    val dataCalcolo = LocalDate.parse(s"$annoCalcolo-$meseCalcolo-03")
    val annomese = "202401"
    val annomese2 = "202402"
    val fileTimestamp = "20240203000000"
    val pivaDistr1 = "piva_distr1"
    val pivaUdd1 = "piva_udd1"
    val pivaDistr2 = "piva_distr2"
    val pivaUdd2 = "piva_udd2"
    val codRemi = "cod_remi1"
    val gdm = "G4"
    val alpha = 35
    val percorsoSalvataggio = "/path/AGG"

    val mid1DettaglioDao = new Mid1DettaglioDaoMock(ds = Seq(
      Mid1DettaglioModel(
        pdr = "pdr1",
        contatore = 2,
        piva_id = pivaDistr1,
        piva_udd = pivaUdd1,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese,
        executionid = executionIdMid1Dettaglio
      ),
      Mid1DettaglioModel(
        pdr = "pdr2",
        contatore = 2,
        piva_id = pivaDistr2,
        piva_udd = pivaUdd1,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese,
        executionid = executionIdMid1Dettaglio
      ),
      Mid1DettaglioModel(
        pdr = "pdr3",
        contatore = 2,
        piva_id = pivaDistr2,
        piva_udd = pivaUdd2,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese,
        executionid = executionIdMid1Dettaglio
      ),
      Mid1DettaglioModel(
        pdr = "pdr4",
        contatore = 2,
        piva_id = pivaDistr2,
        piva_udd = pivaUdd2,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese2,
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
      percorsoSalvataggio = percorsoSalvataggio,
      maxRighePerCsv = 10,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileTimestamp = fileTimestamp
    )
    pubblicazioneMid1FlowMock.run()

    val resultZip = zipWriterDao.ds.cache()
    val resultMidAggregatore = midAggregatoreInfoDao.ds.cache()

    // EXPECTED

    val sep = pubblicazioneMid1FlowMock.csvSeparatore

    val intestazioneCsv = Mid1Model.header(sep)
    val contenutoCsv1 = {
      val contenutoCsv = Mid1Model(PIVA_DISTR = pivaDistr1, PIVA_UDD = pivaUdd1, PDR = "pdr1", ANNOMESE = annomese, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha).toString(sep)
      intestazioneCsv + "\n" + contenutoCsv
    }
    val contenutoCsv2 = {
      val contenutoCsv = Mid1Model(PIVA_DISTR = pivaDistr2, PIVA_UDD = pivaUdd1, PDR = "pdr2", ANNOMESE = annomese, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha).toString(sep)
      intestazioneCsv + "\n" + contenutoCsv
    }
    val contenutoCsv3_4 = {
      val contenutoCsv3 = Mid1Model(PIVA_DISTR = pivaDistr2, PIVA_UDD = pivaUdd2, PDR = "pdr3", ANNOMESE = annomese, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha).toString(sep)
      val contenutoCsv4 = Mid1Model(PIVA_DISTR = pivaDistr2, PIVA_UDD = pivaUdd2, PDR = "pdr4", ANNOMESE = annomese2, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha).toString(sep)
      intestazioneCsv + "\n" + contenutoCsv3 + "\n" + contenutoCsv4
    }


    val expectedZip1 = ZipWriterModel(
      pivaId = pivaDistr1,
      fileName = s"$percorsoSalvataggio/AGG4_$pivaDistr1/$annoCalcolo/$meseCalcolo/${pivaDistr1}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      files = List(
        FileModel(
          fileName = s"${pivaDistr1}_${pivaUdd1}_MID1_${sessioneForzata}_${fileTimestamp}_1.csv",
          content = contenutoCsv1.getBytes()
        ))
    )
    val expectedZip2 = ZipWriterModel(
      pivaId = pivaDistr2,
      fileName = s"$percorsoSalvataggio/AGG4_$pivaDistr2/$annoCalcolo/$meseCalcolo/${pivaDistr2}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      files = List(
        FileModel(
          fileName = s"${pivaDistr2}_${pivaUdd1}_MID1_${sessioneForzata}_${fileTimestamp}_1.csv",
          content = contenutoCsv2.getBytes()
        ),
        FileModel(
          fileName = s"${pivaDistr2}_${pivaUdd2}_MID1_${sessioneForzata}_${fileTimestamp}_1.csv",
          content = contenutoCsv3_4.getBytes()
        )
      )
    )

    val expectedMidAggregatoreInfo1 = MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID1,
      nome_file = s"${pivaDistr1}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      path = s"$percorsoSalvataggio/AGG4_$pivaDistr1/$annoCalcolo/$meseCalcolo/${pivaDistr1}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      tipo_dest = CostantiMid.DESTINAZIONE_MID1,
      piva_dest = pivaDistr1,
      piva_id_file = pivaDistr1,
      piva_udd_file = null,
      data_caricamento = Date.valueOf(dataCalcolo),
      executionid_mid_dettaglio = executionIdMid1Dettaglio,
      executionid = executionId
    )

    val expectedMidAggregatoreInfo2 = MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID1,
      nome_file = s"${pivaDistr2}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      path = s"$percorsoSalvataggio/AGG4_$pivaDistr2/$annoCalcolo/$meseCalcolo/${pivaDistr2}_MID1_${sessioneForzata}_${fileTimestamp}_1.zip",
      tipo_dest = CostantiMid.DESTINAZIONE_MID1,
      piva_dest = pivaDistr2,
      piva_id_file = pivaDistr2,
      piva_udd_file = null,
      data_caricamento = Date.valueOf(dataCalcolo),
      executionid_mid_dettaglio = executionIdMid1Dettaglio,
      executionid = executionId
    )

    // ASSERT

    Assert.assertEquals(expectedZip1, resultZip.where(col(ZipCsvSchema.pivaId) === pivaDistr1).head())
    val resultZip2 = resultZip.where(col(ZipCsvSchema.pivaId) === pivaDistr2).head()
    Assert.assertEquals(expectedZip2.pivaId, resultZip2.pivaId)
    Assert.assertEquals(expectedZip2.fileName, resultZip2.fileName)
    Assert.assertEquals(expectedZip2.files.mkString(""), resultZip2.files.sortBy(x => x.fileName).mkString(""))

    Assert.assertEquals(expectedMidAggregatoreInfo1,resultMidAggregatore.where(col(MidAggregatoreInfoSchema.piva_id_file) === pivaDistr1).head())
    Assert.assertEquals(expectedMidAggregatoreInfo2,resultMidAggregatore.where(col(MidAggregatoreInfoSchema.piva_id_file) === pivaDistr2).head())
  }

}
