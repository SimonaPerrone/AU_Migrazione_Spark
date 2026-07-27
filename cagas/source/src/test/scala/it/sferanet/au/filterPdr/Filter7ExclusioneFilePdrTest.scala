package it.sferanet.au.filterPdr


import it.sferanet.au.EnvironmentSparkTest
import it.sferanet.au.schema.PdrMassivoSchema
import it.sferanet.au.utilities.Environment
import it.sferanet.au.utility.filterPdr.filter7ExclusionFilePdrTest.{check, creator}
import org.apache.spark.sql.DataFrame
import org.junit.Assert.assertEquals
import org.junit.Ignore

import java.security.Permission

@deprecated
@Ignore
class Filter7ExclusioneFilePdrTest extends EnvironmentSparkTest {


  /**
   * test sull'esclusione dei pdr.
   * In input ci sono le misurazioni per i pdr da 1 a 8. Nel file dell'esclusione sono presenti i seguenti pdr:1,3,4,7
   */

  def testFilterPdr(): Unit = {
    Environment.setProperty("ignorePdrMeasure.enable", "true")
    Environment.setProperty("ignorePdrMeasure.pdr.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterPdr/pdr_to_exclude.csv")
    Environment.setProperty("ignorePdrMeasure.measureFile.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterPdr/measure_to_exclude.csv")

    val measures = creator.createMeasures()
    val filteredFlows = new Filter7ExclusioneFilePdr().filter(measures).cache

    /**
     * da escludere
     */
    check.checkIfExsist(filteredFlows, false, "1")
    check.checkIfExsist(filteredFlows, false, "3")
    check.checkIfExsist(filteredFlows, false, "4")
    check.checkIfExsist(filteredFlows, false, "7")

    /**
     * ------------------------------------------------------------
     */
    check.checkIfExsist(filteredFlows, true, "2")
    check.checkIfExsist(filteredFlows, true, "5")
    check.checkIfExsist(filteredFlows, true, "8")

    check.checkNumberFlow(filteredFlows, "1", 0)
    check.checkNumberFlow(filteredFlows, "2", 1)


  }

  /**
   * test sull'esclusione dei pdr massivi.
   * In input ci sono le misurazioni per i pdr da 1 a 8. Nel file dell'esclusione sono presenti i seguenti pdr:1,3,4,7
   */
  def testFilterPdrMassivo(): Unit = {

    Environment.setProperty("ignorePdrMeasure.enable", "true")
    Environment.setProperty("ignorePdrMeasure.pdr.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterPdr/pdr_to_exclude.csv")
    Environment.setProperty("ignorePdrMeasure.measureFile.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterPdr/measure_to_exclude.csv")

    val sqlCtx = Environment.getSqlContext
    import sqlCtx.implicits._

    val dummyPdrMassivo: DataFrame = List(
      Tuple1("1"),
      Tuple1("2"),
      Tuple1("3"),
      Tuple1("4"),
      Tuple1("5"),
      Tuple1("6"),
      Tuple1("7"),
      Tuple1("8")
    ).toDF(PdrMassivoSchema.codice_pdr)

    val filteredPdrMassivo = new Filter7ExclusioneFilePdr().filterPdrMassivo(dummyPdrMassivo)
      .cache()
    //    filteredPdrMassivo.show(false)


    check.checkIfExsist(filteredPdrMassivo, false, "1")
    check.checkIfExsist(filteredPdrMassivo, false, "3")
    check.checkIfExsist(filteredPdrMassivo, false, "4")
    check.checkIfExsist(filteredPdrMassivo, false, "7")
    check.checkIfExsist(filteredPdrMassivo, true, "2")
    check.checkIfExsist(filteredPdrMassivo, true, "5")
    check.checkIfExsist(filteredPdrMassivo, true, "8")

    check.checkNumberFlow(filteredPdrMassivo, "1", 0)
    check.checkNumberFlow(filteredPdrMassivo, "2", 1)

  }

  /**
   * test sull'esclusione delle misure, osservando la lista dei local file, nel mock ci sono
   * sono presenti 3 pdr: (1,2,3).
   * pdr 1 = 2 tgl, per una delle due, è presente il path nel file dell'esclusione => resta una sola misurazione con pdr=1
   * pdr 2 = 1 tgl con il path nel file dell'esclusione => esclusione del pdr 2
   * pdr 3 = 1 tgl senza path => resta la misurazione con pdr=3
   */
  def testFilterEsclusioneFile(): Unit = {

    Environment.setProperty("ignorePdrMeasure.enable", "true")
    Environment.setProperty("ignorePdrMeasure.pdr.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterEsclusioneFile/pdr_to_exclude.csv")
    Environment.setProperty("ignorePdrMeasure.measureFile.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterEsclusioneFile/measure_to_exclude.csv")


    val measures2 = creator.createMeasures2()
    val filteredFlows = new Filter7ExclusioneFilePdr().filter(measures2).cache

    filteredFlows.foreach(println)
    check.checkIfExsist(filteredFlows, true, "1")
    check.checkIfExsist(filteredFlows, false, "2")
    check.checkIfExsist(filteredFlows, true, "3")

    check.checkNumberFlow(filteredFlows, "1", 1)
  }

  /**
   * test sull'esclusione dei pdr e delle misure.
   * File pdr: 1,3,4,7 => nessun flusso collegato a questi prd in output
   * File path: path 1 collegato alla sola e unica misurazione del pdr 8 =>  nessun flusso collegato al pdr 8
   * flussi rimanenti: collegati a pdr: 2,5
   */
  def testFilterEsclusioneFileEdEsclusionePdr(): Unit = {

    Environment.setProperty("ignorePdrMeasure.enable", "true")
    Environment.setProperty("ignorePdrMeasure.pdr.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterEsclusioneFileEdEsclusionePdr/pdr_to_exclude.csv")
    Environment.setProperty("ignorePdrMeasure.measureFile.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterEsclusioneFileEdEsclusionePdr/measure_to_exclude.csv")


    val measures2 = creator.createMeasures3()
    val filteredFlows = new Filter7ExclusioneFilePdr().filter(measures2).cache

    /**
     * da escludere
     */
    filteredFlows.foreach(println)
    check.checkIfExsist(filteredFlows, false, "1")
    check.checkIfExsist(filteredFlows, false, "3")
    check.checkIfExsist(filteredFlows, false, "4")
    check.checkIfExsist(filteredFlows, false, "7")
    check.checkIfExsist(filteredFlows, false, "8")

    /**
     * -----------------------------------------------------------------
     */
    check.checkIfExsist(filteredFlows, true, "2")
    check.checkIfExsist(filteredFlows, true, "5")

  }

  /**
   * ripeto il test precendete, però poichè viene passsato un valore non valido
   * per il paramentro ignorePdrMeasure.enable allora verrò lanciato un ERROR e verrà bloccata l'esecuzione
   */
  @throws[Exception]
  def testIgnorePdrMeasureEnableString(): Unit = {
    try {
      Environment.setProperty("ignorePdrMeasure.enable", "pippo")
      Environment.setProperty("ignorePdrMeasure.pdr.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterEsclusioneFileEdEsclusionePdr/pdr_to_exclude.csv")
      Environment.setProperty("ignorePdrMeasure.measureFile.path", "src/test/resources/inputParamFile/filterPdr/Filter7ExclusioneFilePdrTest/testFilterEsclusioneFileEdEsclusionePdr/measure_to_exclude.csv")


      val measures2 = creator.createMeasures3
      new Filter7ExclusioneFilePdr().filter(measures2).cache
    }
    catch {
      case e: ExitException =>
        assertEquals("Exit status", 1, e.status)
    }

  }

  /** *
   * componenti Gestire la System.exit all'interno del test
   */
  protected class ExitException(val status: Int) extends SecurityException("There is no escape!") {
  }

  private class NoExitSecurityManager extends SecurityManager {
    override def checkPermission(perm: Permission): Unit = {
      // allow anything.
    }

    override def checkPermission(perm: Permission, context: Any): Unit = {
    }

    override def checkExit(status: Int): Unit = {
      super.checkExit(status)
      throw new ExitException(status)
    }
  }

  @throws[Exception]
  override protected def setUp(): Unit = {
    super.setUp
    System.setSecurityManager(new NoExitSecurityManager())
  }

  @throws[Exception]
  override protected def tearDown(): Unit = {
    System.setSecurityManager(null) // or save and restore original

    super.tearDown
  }

  /** ******************************************** */
}



