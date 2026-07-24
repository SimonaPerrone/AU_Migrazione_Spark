package it.au.misure.calcolo_capacita.component.utility.`implicit`

import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant.{errorDataCalcNotCompForm, errorJodaTimeFormat}
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate._
import it.au.misure.calcolo_capacita.utility.ForUnitTest
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.junit.Assert
class ConvertStringIntoDateTest extends ForUnitTest {

  test("test case wrong format") {

    try {
      val dataCalc = "2020-12-12"
      val format = ""
     dataCalc.getLocalDate(format)
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorJodaTimeFormat == e.status)
      }

    }

  }

  test("test case wrong format with date") {
    try {
      val dataCalc = "2020-12-12";
      val format = "yyyy/MM/dd"
      dataCalc.getLocalDate(format)
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorDataCalcNotCompForm == e.status)
      }
    }
  }

  test("test case ") {
    val dataCalcString = "2020/12/12";
    val format = "yyyy/MM/dd"
    val dataCalc=dataCalcString.getLocalDate(format)
    Assert.assertTrue(dataCalc==LocalDate.parse(dataCalcString, DateTimeFormat.forPattern(format)))
  }
}
