package it.sferanet.au.filterPdr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.schema.{PdrMassivoSchema, RcuGasConnessioniDistr2PSchema, VRcuGasDistributorePSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.junit.Assert

class Filter2DistributoreTest extends EnvironmentSparkTest {
  def testFilter(): Unit = {
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
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "6", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "7", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "8", service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, local_file = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])

    val filteredFlows = Filter2DistributoreDummy().filter(measures).cache

    Assert.assertEquals(5, filteredFlows.count())
    Assert.assertEquals(2, filteredFlows.filter(_.pdr == "1").count())
    Assert.assertEquals(1, filteredFlows.filter(_.pdr == "4").count())
    Assert.assertEquals(0, filteredFlows.filter(_.pdr == "2").count())
    Assert.assertEquals(0, filteredFlows.filter(_.pdr == "8").count())
  }

  def testFilterPdrMassivo(): Unit = {
    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val dummyPdrMassivo: DataFrame = List(
      Tuple1("1"),
      Tuple1("2"),
      Tuple1("3"),
      Tuple1("4"),
      Tuple1("5"),
      Tuple1("6"),
      Tuple1("7")
    ).toDF(PdrMassivoSchema.codice_pdr)

    val filteredPdrMassivo = Filter2DistributoreDummy().filterPdrMassivo(dummyPdrMassivo)

    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "7").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "6").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "5").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "4").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "3").count())
    Assert.assertEquals(0, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "2").count())
    Assert.assertEquals(1, filteredPdrMassivo.where(col(PdrMassivoSchema.codice_pdr) === "1").count())
  }

  case class Filter2DistributoreDummy() extends Filter2Distributore {

    override def getRcuGasConnessioniDistr2P: DataFrame = {
      val sqlContext = Environment.getSqlContext
      import sqlContext.implicits._

      List(
        ("d1", "1", "2020-01-01", "2100-01-01"),
        ("d1", "3", "2020-01-01", "2100-01-01"),
        ("d1", "4", null, null),
        ("d2", "5", "2020-01-01", "2100-01-01"),
        ("d2", "6", "2020-01-01", "2020-01-02"),
        ("d3", "8", "2020-01-01", "2100-01-01")
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
        ("p3", "d3")
      ).toDF(
        VRcuGasDistributorePSchema.t_piva,
        VRcuGasDistributorePSchema.n_id_distributore
      )
    }
  }

}
