package it.eng.au.mid.pubblicazione.mid2

import it.eng.au.mid.EnvironmentSparkTest
import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.pubblicazione.PredisposizioneMid2Flow
import it.eng.au.mid.model.hive.atg.AtgVariazioniSocModel
import it.eng.au.mid.model.hive.mid.Mid2DettaglioModel
import it.eng.au.mid.model.hive.rcu.RcuAziendaPModel
import it.eng.au.mid.model.hive.rcugas.RcugasConnessioniDistr2RemiPModel
import it.eng.au.mid.schema.hive.mid.Mid2DettaglioSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.junit.Assert

class PredisposizioneMid2FlowTest extends EnvironmentSparkTest {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  def testCalcolaDistributoreCorrente(): Unit = {
    val midDettaglioDs = Seq(Mid2DettaglioModel(pdr = "1")).toDS()
    val connessioniDs = Seq(
      RcugasConnessioniDistr2RemiPModel(t_codice_pdr= "1", t_piva_distr = "att")
    ).toDS()

    val result = new PredisposizioneMid2Flow().calcolaDistributoreCorrente(midDettaglioDs, connessioniDs)
    val expected = Mid2DettaglioModel(pdr = "1", piva_distr_att = "att")

    Assert.assertEquals(expected, result.head())
  }

  def testCalcolaDistributoreAttivoSospeso(): Unit = {
    val midDettaglioDs = Seq(
      Mid2DettaglioModel(pdr = "1", piva_id = "piva_cess"),
      Mid2DettaglioModel(pdr = "2", piva_id = "piva_att")
    ).toDS()
    val cessazioniDs = Seq(
      AtgVariazioniSocModel(t_piva_distr = "piva_cess")
    ).toDS()

    val result = new PredisposizioneMid2Flow().calcolaDistributoreAttivoSospeso(midDettaglioDs, cessazioniDs).cache()

    Assert.assertEquals(2, result.count())
    Assert.assertEquals(CostantiMid.SOC_SOSPESO, result.where(col(Mid2DettaglioSchema.pdr) === "1").head().stato_id)
    Assert.assertEquals(CostantiMid.SOC_ATTIVO, result.where(col(Mid2DettaglioSchema.pdr) === "2").head().stato_id)
  }

  def testAggiungiInformazioniAnagrafica(): Unit = {
    val mid2Dettaglio = Seq(
      Mid2DettaglioModel(piva_id = "123", piva_udd = "456")
    ).toDS()
    val distributori = Seq(
      RcuAziendaPModel(t_piva = "123", t_rag_soc = "compagnia_1"),
      RcuAziendaPModel(t_piva = "456", t_rag_soc = "compagnia_2")
    ).toDS()

    val result = new PredisposizioneMid2Flow().aggiungiInformazioniRagioneSociale(mid2Dettaglio, distributori).cache()

    Assert.assertEquals("compagnia_1", result.head().rag_soc_id)
    Assert.assertEquals("compagnia_2", result.head().rag_soc_udd)
  }
}
