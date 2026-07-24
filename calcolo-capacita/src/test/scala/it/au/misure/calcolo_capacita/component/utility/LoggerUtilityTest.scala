package it.au.misure.calcolo_capacita.component.utility

import it.au.misure.calcolo_capacita.utility.{Checker, ForUnitTest}

class LoggerUtilityTest extends ForUnitTest
  with Checker {

  test("test") {
    LoggerUtility.printInfo("hello",getClass.getName)
  }
}