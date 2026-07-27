package it.sferanet.au.filterPdr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.schema.{CaPreFinalSchema, PdrMassivoSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.junit.{Assert, Ignore}

@deprecated
@Ignore
class Filter6CalculateCaForcingTest extends EnvironmentSparkTest {
  object check {
    /**
     *
     * @param df       dataframe su cui effettuare il controllo
     * @param excepted true se il pdf dovrà essere presente dopo il filtro
     *                 false altrimenti
     * @param codPdr   cod_pdr da controllare
     */
    def checkIfExsist(df: DataFrame, excepted: Boolean, codPdr: Int): Unit = {
      Assert.assertEquals(if (excepted) 1 else 0, df.where(col(PdrMassivoSchema.codice_pdr) === codPdr).count())
    }

    /**
     *
     * @param flow               Rdd di @link Flow su cui effettuare il controllo
     * @param numberFlowAspected numero di flow aspettato
     */
    def checkNumberFlow(flow: RDD[Flow], numberFlowAspected: Int): Unit = {
      Assert.assertEquals(numberFlowAspected, flow.count())
    }

    /**
     *
     * @param df                 dataframe su cui effettuare il controllo
     * @param numberFlowAspected numero di flow aspettato
     */
    def checkNumberFlow(df: DataFrame, numberFlowAspected: Int): Unit = {
      Assert.assertEquals(numberFlowAspected, df.count())
    }

    /**
     *
     * @param df      dataframe su cui effettuare il controllo
     * @param codPdr  da controllare
     * @param ca      ca aspettato per @link codPdr
     * @param codPrel codPrel aspettato per @link codPdr
     */
    def checkConstantField(df: DataFrame, codPdr: Int, ca: Double, codPrel: String): Unit = {
      val firstRow = df.filter(col(PdrMassivoSchema.codice_pdr) === lit(codPdr)).collect()(0)
      Assert.assertEquals(ca.toString, firstRow.getAs[String](CaPreFinalSchema.prelievo_annuo_prev_forced))
      Assert.assertEquals(codPrel, firstRow.getAs[String](CaPreFinalSchema.cod_prof_prel_std_forced))
    }
  }

  def testFilter(): Unit = {
    Environment.setProperty("filterPdr.forzatura.path", "src/test/resources/pdr_forzatura.csv")

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

    val filteredFlows = new Filter6CalculateCaForcing().filter(measures).cache

    check.checkNumberFlow(filteredFlows, 5)
    check.checkNumberFlow(filteredFlows.filter(_.pdr == "1"), 2)
    check.checkNumberFlow(filteredFlows.filter(_.pdr == "2"), 0)

  }

  def testFilterPdrMassivo(): Unit = {
    Environment.setProperty("filterPdr.forzatura.path", "src/test/resources/pdr_forzatura.csv")

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

    val filteredPdrMassivo = new Filter6CalculateCaForcing().filterPdrMassivo(dummyPdrMassivo)
      .cache()

    check.checkNumberFlow(filteredPdrMassivo, 4)

    check.checkConstantField(filteredPdrMassivo, 1, 12, "a1")
    check.checkConstantField(filteredPdrMassivo, 3, 13, "a2")
    check.checkConstantField(filteredPdrMassivo, 4, 14, "a3")
    check.checkConstantField(filteredPdrMassivo, 7, 15, "a4")

    check.checkIfExsist(filteredPdrMassivo, true, 7)
    check.checkIfExsist(filteredPdrMassivo, false, 6)
    check.checkIfExsist(filteredPdrMassivo, false, 5)
    check.checkIfExsist(filteredPdrMassivo, true, 4)
    check.checkIfExsist(filteredPdrMassivo, true, 3)
    check.checkIfExsist(filteredPdrMassivo, false, 2)
    check.checkIfExsist(filteredPdrMassivo, true, 1)

  }
}


