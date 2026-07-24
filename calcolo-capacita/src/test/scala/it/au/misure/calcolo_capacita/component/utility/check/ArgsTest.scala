package it.au.misure.calcolo_capacita.component.utility.check


import it.au.misure.calcolo_capacita.component.utility.ApplicationConstant._
import it.au.misure.calcolo_capacita.component.utility.`implicit`.ConvertStringIntoDate.ConvertString
import it.au.misure.calcolo_capacita.component.utility.property.ApplicationProperty.format
import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.junit.Assert

class ArgsTest
  extends ForUnitTest
  with Checker {

  test("test case ok") {
    val obj = Args(Array("2020/12/01", "10", "11", "false", "false"))
    Assert.assertTrue(obj.dataCalc == LocalDate.parse("2020/12/01", DateTimeFormat.forPattern(format)))
    Assert.assertTrue(obj.x == 10)
    Assert.assertTrue(obj.y == 11)
    Assert.assertTrue(obj.verbose == false)
  }

  test("test case oK DataCalc WrongFormat") {
    try {
      Args(Array("202012/01", "10", "11", "false", "false"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorDataCalcNotCompForm == e.status)
      }

    }
  }

  test("test case oK o x not Integer") {
    try {
      Args(Array("2020/12/01", "10a", "11", "false", "false"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorParamNotInt == e.status)
      }

    }
  }

  test("test case oK o y not Integer") {
    try {
      Args(Array("2020/12/01", "10", "11a", "false", "false"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorParamNotInt == e.status)
      }

    }
  }

  test("test case ko verbose not boolean false") {
    try {
      Args(Array("2020/12/01", "10", "11", "false", "falsea"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorParamNotBoolean == e.status)
      }

    }
  }

  test("test case ko verbose not boolean true") {
    try {
      Args(Array("2020/12/01", "10", "11", "false", "truea"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorParamNotBoolean == e.status)
      }

    }
  }

  test("test case ko number param input") {
    try {
      Args(Array("2020/12/01", "10", "11", "false", "true", ""))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorNumberParam == e.status)
      }

    }
  }

  test("test case ko x not conformed 0") {
    try {
      Args(Array("2020/12/01", "0", "11", "false", "true"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorXAtLeast2 == e.status)
      }

    }
  }

  test("test case ko x not conformed1") {
    try {
      Args(Array("2020/12/01", "1", "11", "false", "true"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorXAtLeast2 == e.status)
      }

    }
  }

  test("test case automatic true") {

    val p = Args(Array("2020/12/12", "10", "10", "true", "true"))
    print(p.dataCalc)
    Assert.assertTrue(p.dataCalc == LocalDate.now().toString(format).getLocalDate(format))
  }

  test("test case automatic false") {

    val p = Args(Array("2020/12/12", "10", "10", "false", "true"))
    Assert.assertTrue(p.dataCalc == LocalDate.parse("2020/12/12", DateTimeFormat.forPattern("yyyy/MM/dd")))
  }

  test("test case automatic wrong param") {
    try {
      Args(Array("2020/12/01", "1", "11", "truea", "true"))
    }
    catch {
      case e: ExitException => {
        Assert.assertTrue(errorParamNotBoolean == e.status)
      }

    }
  }
}