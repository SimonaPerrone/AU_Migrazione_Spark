package it.sferanet.au.filterPdr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.schema._
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel
import org.junit.Assert

class Filter4UDBTest extends EnvironmentSparkTest {
  def testFilterMeasures(): Unit = {
    val measures = Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "1", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "2", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "3", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "4", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "5", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val filteredFlows = Filter4UDBDummy().filter(measures).persist(StorageLevel.MEMORY_AND_DISK)

    Assert.assertEquals(5, filteredFlows.count())
    Assert.assertEquals(2, filteredFlows.filter(_.pdr == "1").count())
    Assert.assertEquals(1, filteredFlows.filter(_.pdr == "2").count())
    Assert.assertEquals(1, filteredFlows.filter(_.pdr == "3").count())
    Assert.assertEquals(1, filteredFlows.filter(_.pdr == "4").count())
    Assert.assertEquals(0, filteredFlows.filter(_.pdr == "5").count())
  }

  def testFilterPdrMassivo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val dummyPdrMassivo: DataFrame = List(
      ("1", "id1"),
      ("2", "id26"),
      ("3", "id13"),
      ("4", "id64"),
      ("5", "id50")
    ).toDF(PdrMassivoSchema.codice_pdr, PdrMassivoSchema.n_id_udb)

    val filteredPdrMassivo = Filter4UDBDummy().filterPdrMassivo(dummyPdrMassivo)

    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "5").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "4").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "3").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "2").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "1").count())

  }

  case class Filter4UDBDummy() extends Filter4UDB {

    override def getRcuGasBilanciamentoP: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._
      List(
        ("id_udb_1", "2020-01-01", "2100-01-01", "id_pdr_1"),
        ("id_udb_2", "2020-01-01", "2100-01-01", "id_pdr_2"),
        ("id_udb_3", "2020-01-01", "2100-01-01", "id_pdr_3"),
        ("id_udb_4", "2020-01-01", "2100-01-01", "id_pdr_4"),
        ("id_udb_5", "2020-01-01", "2100-01-01", "id_pdr_5")
      ).toDF(RcuGasBilanciamentoPSchema.n_id_udb
        , RcuGasBilanciamentoPSchema.d_data_inizio
        , RcuGasBilanciamentoPSchema.d_data_fine
        , RcuGasBilanciamentoPSchema.n_id_pdr)
    }

    override def getRcuAziendaP: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._
      List(
        ("piva_udb_1", "id_azienda_1"),
        ("piva_udb_2", "id_azienda_2"),
        ("piva_udb_3", "id_azienda_3"),
        ("piva_udb_4", "id_azienda_4")
      ).toDF(
        RcuAziendaPSchema.t_piva,
        RcuAziendaPSchema.n_id_azienda
      )
    }

    override def getRcuGasUdbP: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._
      List(
        ("id_udb_1", "id_azienda_1"),
        ("id_udb_2", "id_azienda_2"),
        ("id_udb_3", "id_azienda_3"),
        ("id_udb_4", "id_azienda_4")
      ).toDF(
        RcuGasUdbPSchema.n_id_udb,
        RcuGasUdbPSchema.n_id_azienda
      )
    }

    override def getRcuGasMassivoP: DataFrame = {
      val sqlCtx = Environment.getSqlContext
      import sqlCtx.implicits._
      List(
        ("id_pdr_1", "1", "u1", "2020-01-01", "2100-01-01", ""),
        ("id_pdr_2", "2", "u2", "2020-01-01", "2100-01-01", ""),
        ("id_pdr_3", "3", "u1", "2020-01-01", "2100-01-01", ""),
        ("id_pdr_4", "4", "u2", "2020-01-01", "2100-01-01", "")
      ).toDF(
        RcuGasMassivoPSchema.n_id_pdr,
        RcuGasMassivoPSchema.t_codice_pdr,
        RcuGasMassivoPSchema.piva_udd,
        RcuGasMassivoPSchema.d_data_inizio_for,
        RcuGasMassivoPSchema.data_fine_for,
        RcuGasMassivoPSchema.n_id_fornitura
      )

    }

    override def getRcuGasConnessioniDistr2P: DataFrame = {
      val sqlContext = Environment.getSqlContext
      import sqlContext.implicits._

      List(
        ("d1", "1", "2020-01-01", "2100-01-01"),
        ("d2", "2", "2020-01-01", "2100-01-01"),
        ("d3", "3", "2020-01-01", "2100-01-01"),
        ("d4", "4", "2020-01-01", "2100-01-01")
      ).toDF(
        RcuGasConnessioniDistr2PSchema.n_id_distr,
        RcuGasConnessioniDistr2PSchema.t_codice_pdr,
        RcuGasConnessioniDistr2PSchema.d_data_inizio_conn,
        RcuGasConnessioniDistr2PSchema.d_data_fine_conn
      )
    }

    override def getVRcuGasDistributoreP: DataFrame = {
      val sqlContext = Environment.getSqlContext
      import sqlContext.implicits._

      List(
        ("p1", "d1"),
        ("p2", "d2"),
        ("p2", "d3"),
        ("p1", "d4")
      ).toDF(
        VRcuGasDistributorePSchema.t_piva,
        VRcuGasDistributorePSchema.n_id_distributore
      )
    }
  }

}
