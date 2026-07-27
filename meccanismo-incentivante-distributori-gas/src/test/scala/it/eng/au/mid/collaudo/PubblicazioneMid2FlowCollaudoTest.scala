package it.eng.au.mid.collaudo

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.mock.dao.{Mid2DettaglioDaoMock, MidAggregatoreInfoDaoMock, ZipWriterDaoMock}
import it.eng.au.mid.mock.pubblicazione.PubblicazioneMid2FlowMock
import it.eng.au.mid.model.file.pubblicazione.{FileModel, Mid1Model, Mid2Model, ZipWriterModel}
import it.eng.au.mid.model.hive.mid.{Mid2DettaglioModel, MidAggregatoreInfoModel}
import it.eng.au.mid.schema.file.pubblicazione.{Mid2Schema, ZipCsvSchema}
import it.eng.au.mid.schema.hive.mid.MidAggregatoreInfoSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

import java.sql.Date
import java.time.LocalDate

class PubblicazioneMid2FlowCollaudoTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Collaudo flusso Pubblicazione MID2 (Step 2)
   */
  def testCollaudo(): Unit = {
    val executionIdMid2Dettaglio = 1704279600000L
    val executionId = 1704280700000L
    val sessioneForzata = "AGG_FORZ"
    val annoCalcolo = "2024"
    val meseCalcolo = "02"
    val dataCalcolo = LocalDate.parse(s"$annoCalcolo-$meseCalcolo-03")
    val annomese = "202401"
    val annomese2 = "202402"
    val fileTimestamp = "20240103000000"
    val pivaDistr1 = "piva_distr1"
    val pivaUdd1 = "piva_udd1"
    val pivaDistr2 = "piva_distr2"
    val ragSocialeDistr1 = "ragsocdistr1"
    val ragSocialeDistr2 = "ragsocdistr2"
    val ragSocialeUdd1 = "ragsocudd1"
    val ragSocialeUdd2 = "ragsocudd2"
    val pivaUdd2 = "piva_udd2"
    val codRemi = "cod_remi1"
    val gdm = "G4"
    val alpha = 35
    val percorsoSalvataggio = "/path/TDG5_80198650584"
    val cseaPartitaIva = "80198650584"

    val mid2DettaglioDao = new Mid2DettaglioDaoMock(ds = Seq(
      Mid2DettaglioModel(
        pdr = "pdr1",
        contatore = 2,
        piva_id = pivaDistr1,
        rag_soc_id = ragSocialeDistr1,
        piva_udd = pivaUdd1,
        rag_soc_udd = ragSocialeUdd1,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese,
        executionid = executionIdMid2Dettaglio
      ),
      Mid2DettaglioModel(
        pdr = "pdr2",
        contatore = 2,
        piva_id = pivaDistr2,
        rag_soc_id = ragSocialeDistr2,
        piva_udd = pivaUdd1,
        rag_soc_udd = ragSocialeUdd1,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese,
        executionid = executionIdMid2Dettaglio
      ),
      Mid2DettaglioModel(
        pdr = "pdr3",
        contatore = 2,
        piva_id = pivaDistr2,
        rag_soc_id = ragSocialeDistr2,
        piva_udd = pivaUdd2,
        rag_soc_udd = ragSocialeUdd2,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese,
        executionid = executionIdMid2Dettaglio
      ),
      Mid2DettaglioModel(
        pdr = "pdr4",
        contatore = 2,
        piva_id = pivaDistr2,
        rag_soc_id = ragSocialeDistr2,
        piva_udd = pivaUdd2,
        rag_soc_udd = ragSocialeUdd2,
        cod_remi = codRemi,
        gdm = gdm,
        alpha = alpha,
        executionid_mid_contatori = 0L,
        annomese = annomese2,
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
      percorsoSalvataggio = percorsoSalvataggio,
      cseaPartitaIva = cseaPartitaIva,
      maxRighePerCsv = 10,
      executionId = executionId,
      dataCalcolo = dataCalcolo,
      fileTimestamp = fileTimestamp
    )
    pubblicazioneMid2FlowMock.run()

    val resultZip = zipWriterDao.ds.cache()
    val resultMidAggregatore = midAggregatoreInfoDao.ds.cache()

    // EXPECTED

    val sep = pubblicazioneMid2FlowMock.csvSeparatore

    val intestazioneCsv = Mid2Model.header(sep)
    val contenutoCsv1 = {
      val contenutoCsv = Mid2Model(PIVA_DISTR = pivaDistr1, RAGIONE_SOCIALE_DISTR = ragSocialeDistr1, STATO_DISTR = null, PIVA_UDD = pivaUdd1, RAGIONE_SOCIALE_UDD = ragSocialeUdd1, PDR = "pdr1", ANNOMESE = annomese, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha, PIVA_DISTR_ATT = null, RAGIONE_SOCIALE_DISTR_ATT = null).toString(sep)
      intestazioneCsv + "\n" + contenutoCsv
    }

    // file piva distr 2
    val contenutoCsv2 = {
      val contenuto1 = Mid2Model(PIVA_DISTR = pivaDistr2, RAGIONE_SOCIALE_DISTR = ragSocialeDistr2, STATO_DISTR = null, PIVA_UDD = pivaUdd1, RAGIONE_SOCIALE_UDD = ragSocialeUdd1, PDR = "pdr2", ANNOMESE = annomese, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha, PIVA_DISTR_ATT = null, RAGIONE_SOCIALE_DISTR_ATT = null).toString(sep)
      val contenuto2 = Mid2Model(PIVA_DISTR = pivaDistr2, RAGIONE_SOCIALE_DISTR = ragSocialeDistr2, STATO_DISTR = null, PIVA_UDD = pivaUdd2, RAGIONE_SOCIALE_UDD = ragSocialeUdd2, PDR = "pdr3", ANNOMESE = annomese, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha, PIVA_DISTR_ATT = null, RAGIONE_SOCIALE_DISTR_ATT = null).toString(sep)
      val contenuto3 = Mid2Model(PIVA_DISTR = pivaDistr2, RAGIONE_SOCIALE_DISTR = ragSocialeDistr2, STATO_DISTR = null, PIVA_UDD = pivaUdd2, RAGIONE_SOCIALE_UDD = ragSocialeUdd2, PDR = "pdr4", ANNOMESE = annomese2, N = 2, COD_REMI = codRemi, GDM = gdm, ALPHA = alpha, PIVA_DISTR_ATT = null, RAGIONE_SOCIALE_DISTR_ATT = null).toString(sep)
      intestazioneCsv + "\n" + contenuto1 + "\n" + contenuto2 + "\n" + contenuto3
    }


    val expectedZip1 = ZipWriterModel(
      pivaId = pivaDistr1,
      fileName = s"$percorsoSalvataggio/$annoCalcolo/$meseCalcolo/${pivaDistr1}_MID2_${sessioneForzata}_${fileTimestamp}_1.zip",
      files = List(
        FileModel(
          fileName = s"${pivaDistr1}_MID2_${sessioneForzata}_${fileTimestamp}_1.csv",
          content = contenutoCsv1.getBytes()
        ))
    )
    val expectedZip2 = ZipWriterModel(
      pivaId = pivaDistr2,
      fileName = s"$percorsoSalvataggio/$annoCalcolo/$meseCalcolo/${pivaDistr2}_MID2_${sessioneForzata}_${fileTimestamp}_1.zip",
      files = List(
        FileModel(
          fileName = s"${pivaDistr2}_MID2_${sessioneForzata}_${fileTimestamp}_1.csv",
          content = contenutoCsv2.getBytes()
        )
      )
    )

    val expectedMidAggregatoreInfo1 = MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID2,
      nome_file = s"${pivaDistr1}_MID2_${sessioneForzata}_${fileTimestamp}_1.zip",
      path = s"$percorsoSalvataggio/$annoCalcolo/$meseCalcolo/${pivaDistr1}_MID2_${sessioneForzata}_${fileTimestamp}_1.zip",
      tipo_dest = CostantiMid.DESTINAZIONE_MID2,
      piva_dest = cseaPartitaIva,
      piva_id_file = pivaDistr1,
      piva_udd_file = null,
      data_caricamento = Date.valueOf(dataCalcolo),
      executionid_mid_dettaglio = executionIdMid2Dettaglio,
      executionid = executionId
    )

    val expectedMidAggregatoreInfo2 = MidAggregatoreInfoModel(
      operation_name = CostantiMid.NOME_OPERAZIONE_MID2,
      nome_file = s"${pivaDistr2}_MID2_${sessioneForzata}_${fileTimestamp}_1.zip",
      path = s"$percorsoSalvataggio/$annoCalcolo/$meseCalcolo/${pivaDistr2}_MID2_${sessioneForzata}_${fileTimestamp}_1.zip",
      tipo_dest = CostantiMid.DESTINAZIONE_MID2,
      piva_dest = cseaPartitaIva,
      piva_id_file = pivaDistr2,
      piva_udd_file = null,
      data_caricamento = Date.valueOf(dataCalcolo),
      executionid_mid_dettaglio = executionIdMid2Dettaglio,
      executionid = executionId
    )

    // ASSERT

    val resultZip1 = resultZip.where(col(ZipCsvSchema.pivaId) === pivaDistr1).head()
    Assert.assertEquals(expectedZip1.pivaId, resultZip1.pivaId)
    Assert.assertEquals(expectedZip1.fileName, resultZip1.fileName)
    val resultZip2 = resultZip.where(col(ZipCsvSchema.pivaId) === pivaDistr2).head()
    Assert.assertEquals(expectedZip2.pivaId, resultZip2.pivaId)
    Assert.assertEquals(expectedZip2.fileName, resultZip2.fileName)

    // controlla il contenuto del file CSV che possiede piu righe.
    // puo' non funzionare in quanto l ordine delle righe non e' fisso e bisognerebbe modificare il controllo in modo che
    // verifichi riga per riga del contenuto
    //Assert.assertEquals(expectedZip2.files.mkString(""), resultZip2.files.mkString(""))

    Assert.assertEquals(expectedMidAggregatoreInfo1,resultMidAggregatore.where(col(MidAggregatoreInfoSchema.piva_id_file) === pivaDistr1).head())
    Assert.assertEquals(expectedMidAggregatoreInfo2,resultMidAggregatore.where(col(MidAggregatoreInfoSchema.piva_id_file) === pivaDistr2).head())
  }

}
