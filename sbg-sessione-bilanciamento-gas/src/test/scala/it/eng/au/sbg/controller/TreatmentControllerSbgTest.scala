package it.eng.au.sbg.controller

import it.eng.au.aggiustamentoGas.model.agg.PdrWithMonthTreatmentYSBG
import it.eng.au.aggiustamentoGas.model.rcugas.RcuGasVarTrattamentoP
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.sbg.EnvironmentSparkTest
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class TreatmentControllerSbgTest extends EnvironmentSparkTest {
  def testCalc(): Unit = {
    val formatter = DateTimeFormat.forPattern("dd-MM-yyyy")

    val rcuGasTrattamento = Environment.getSpark.sparkContext.parallelize(List(
      RcuGasVarTrattamentoP(codicePdr = "1", dataInizio = formatter.parseDateTime("01-01-2022"), dataFine = formatter.parseDateTime("01-02-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "1", dataInizio = formatter.parseDateTime("02-02-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.G)
      , RcuGasVarTrattamentoP(codicePdr = "2", dataInizio = formatter.parseDateTime("01-01-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "3", dataInizio = formatter.parseDateTime("01-03-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "4", dataInizio = formatter.parseDateTime("01-04-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "5", dataInizio = formatter.parseDateTime("15-03-2022"), dataFine = formatter.parseDateTime("01-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "6", dataInizio = formatter.parseDateTime("15-01-2022"), dataFine = formatter.parseDateTime("15-03-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "7", dataInizio = formatter.parseDateTime("03-04-2022"), dataFine = formatter.parseDateTime("15-04-2022"), tTrattamentoSettlement = Treatment.G)
      , RcuGasVarTrattamentoP(codicePdr = "7", dataInizio = formatter.parseDateTime("16-04-2022"), dataFine = formatter.parseDateTime("20-04-2022"), tTrattamentoSettlement = Treatment.M)
      , RcuGasVarTrattamentoP(codicePdr = "7", dataInizio = formatter.parseDateTime("21-04-2022"), dataFine = formatter.parseDateTime("28-04-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "8", dataInizio = formatter.parseDateTime("01-05-2022"), dataFine = formatter.parseDateTime("28-08-2022"), tTrattamentoSettlement = Treatment.Y)
      , RcuGasVarTrattamentoP(codicePdr = "9", dataInizio = formatter.parseDateTime("30-04-2022"), dataFine = formatter.parseDateTime("28-08-2022"), tTrattamentoSettlement = Treatment.Y)
    ))

    val treatmentController = new TreatmentControllerSbg

    val result = treatmentController.calc(rcuGasTrattamento, "202202", "202204").cache()

    result.sortBy(treat => (treat.pdr, treat.month)).collect.foreach(println)

    Assert.assertEquals(result.filter(treat => treat.pdr == "1" && treat.month == "202202").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "1" && treat.month == "202203").map(_.treatment).collect().head, "G")
    Assert.assertEquals(result.filter(treat => treat.pdr == "1" && treat.month == "202204").map(_.treatment).collect().head, "G")
    Assert.assertEquals(result.filter(treat => treat.pdr == "2" && treat.month == "202202").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "2" && treat.month == "202203").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "2" && treat.month == "202204").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "3" && treat.month == "202202").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "3" && treat.month == "202203").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "3" && treat.month == "202204").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "4" && treat.month == "202202").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "4" && treat.month == "202203").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "4" && treat.month == "202204").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "5" && treat.month == "202202").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "5" && treat.month == "202203").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "5" && treat.month == "202204").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "6" && treat.month == "202202").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "6" && treat.month == "202203").map(_.treatment).collect().head, "Y")
    Assert.assertEquals(result.filter(treat => treat.pdr == "6" && treat.month == "202204").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "7" && treat.month == "202202").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "7" && treat.month == "202203").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "7" && treat.month == "202204").map(_.treatment).collect().head, "G")
    Assert.assertEquals(result.filter(treat => treat.pdr == "8" && treat.month == "202202").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "8" && treat.month == "202203").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "8" && treat.month == "202204").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "9" && treat.month == "202202").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "9" && treat.month == "202203").map(_.treatment).collect().head, "N")
    Assert.assertEquals(result.filter(treat => treat.pdr == "9" && treat.month == "202204").map(_.treatment).collect().head, "Y")
  }

  def testGenerateMonthTreatmentOnPdrWithTreatmentY(): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val ds = sqlContext.createDataset(List(
      PdrWithMonthTreatmentYSBG("pdr1")
      , PdrWithMonthTreatmentYSBG("pdr2")
      , PdrWithMonthTreatmentYSBG("pdr3")
    ))

    val treatmentController = new TreatmentControllerSbg

    val result = treatmentController.generateMonthTreatmentOnPdrWithTreatmentY(ds, "202201").cache()

    result.collect.foreach(println)
  }

}
