package it.au.misure.calcolo_capacita.component.implementation

import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate._
import it.au.misure.calcolo_capacita.component.utility.check.Args
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.junit.Assert

class CalculationTest
  extends ForUnitTest
    with Checker {


  test("test calculateDataInizio dataCalc = 2021/03/08") {

    val dataCalc = "2021/03/08".getLocalDate("yyyy/MM/dd")
    implicit val args = Args(dataCalc = dataCalc, x = 0, y = 0, automatic = false, verbose = true, executionId = "")

    Assert.assertTrue(Calculation.calculateDataInizio().toString("yyyy-MM-dd") === "2021-04-01")
  }
  test("test calculateDataInizio dataCalc = 2021/02/28") {

    val dataCalc = "2021/02/28".getLocalDate("yyyy/MM/dd")
    implicit val args = Args(dataCalc = dataCalc, x = 0, y = 0, automatic = false, verbose = true, executionId = "")

    Assert.assertTrue(Calculation.calculateDataInizio().toString("yyyy-MM-dd") === "2021-03-01")
  }
  test("test calculateAnnoTermico d_data_inizo = 2021/02/28") {

    val dDataInizio = "2021/02/28".getLocalDate("yyyy/MM/dd")

    Assert.assertTrue(Calculation.calculateAnnoTermico(dDataInizio) === 2021)
  }
  test("test calculateAnnoTermico d_data_inizo = 2021/10/28") {

    val dDataInizio = "2021/10/28".getLocalDate("yyyy/MM/dd")

    Assert.assertTrue(Calculation.calculateAnnoTermico(dDataInizio) === 2022)
  }
}
