package it.sferanet.au.filterPdr

import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.model.Flow
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.model.rettifica.Rgl
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.junit.Assert

class MeasureFilterTest extends EnvironmentSparkTest {

  val measures: RDD[Flow] = createTestMeasures

  val pdrEqualsOne = (flow: Flow) => {
    flow.pdr.equals("1")
  }
  val pdrEqualsTwo = (flow: Flow) => {
    flow.pdr.equals("2")
  }
  val pdrEqualsThree = (flow: Flow) => {
    flow.pdr.equals("3")
  }
  val pdrEqualsSix = (flow: Flow) => {
    flow.pdr.equals("6")
  }
  val pdrEqualsTen = (flow: Flow) => {
    flow.pdr.equals("10")
  }

  def testMeasureFilter(): Unit = {
    //Enable the filter
    Environment.setProperty("ignorePdrMeasure.enable", "true")

    val filter = MeasureFilter
    val filteredRDDWithBC = filter.excludeMeasures(measures)

    Assert.assertEquals(0, filteredRDDWithBC.filter(pdrEqualsOne).count())
    Assert.assertEquals(0, filteredRDDWithBC.filter(pdrEqualsTwo).count())
    Assert.assertEquals(1, filteredRDDWithBC.filter(pdrEqualsThree).count())
    Assert.assertEquals(1, filteredRDDWithBC.filter(pdrEqualsSix).count())
    Assert.assertEquals(0, filteredRDDWithBC.filter(pdrEqualsTen).count())
    Assert.assertEquals(2, filteredRDDWithBC.count())

    Environment.setProperty("ignorePdrMeasure.broadcast.threshold", "-1")
    val filteredRDDWith3Join = filter.excludeMeasures(measures)

    Assert.assertEquals(0, filteredRDDWith3Join.filter(pdrEqualsOne).count())
    Assert.assertEquals(0, filteredRDDWith3Join.filter(pdrEqualsTwo).count())
    Assert.assertEquals(1, filteredRDDWith3Join.filter(pdrEqualsThree).count())
    Assert.assertEquals(1, filteredRDDWith3Join.filter(pdrEqualsSix).count())
    Assert.assertEquals(0, filteredRDDWith3Join.filter(pdrEqualsTen).count())
    Assert.assertEquals(2, filteredRDDWith3Join.count())

    //Disable the filter
    Environment.setProperty("ignorePdrMeasure.enable", "false")
    Assert.assertEquals(measures.count(), filter.excludeMeasures(measures).count())
    //Is it robust?
    Environment.setProperty("ignorePdrMeasure.enable", "nicolaDiSanto")
    Assert.assertEquals(measures.count(), filter.excludeMeasures(measures).count())
  }

  private def createTestMeasures: RDD[Flow] = {
    val (f1, f2, f3, f4, f7) = (Option("f1"), Option("f2"), Option("f3"), Option("f4"), Option("f7"))

    Environment.getSparkContext.parallelize(List(
      Tgl(pdr = "1", local_file = f1, service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Rgl(pdr = "1", local_file = f2, service = null, date = null, collected = Some("AC"), measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, d_caricamento = None, motivation = Some(0), isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "2", local_file = f1, service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "3", local_file = f4, service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "6", local_file = f7, service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None),
      Tgl(pdr = "10", local_file = f3, service = null, date = null, readType = Some('a'), isValid = null, measure = null,
        converted = null, serialNumberConv = null, serialNumberMis = null, d_caricamento = None, isNewRoute = false, pivaDistr = None, pivaUtente = None, ammissibilita = None)
    )).map(_.asInstanceOf[Flow])
  }

}
