package it.au.misure.calcolo_capacita.utility.test_case

object CreatorFactory {


  val testCase1: String = "test_case_1"
  val testCase2: String = "test_case_2"
  val testCase3: String = "test_case_3"
  val testCase4: String = "test_case_4"
  val testCase5: String = "test_case_5"
  val testCase7: String = "test_case_7"
  val testCasea1: String = "test_case_a1"
  val testCasea2: String = "test_case_a2"

  def getTestCreator(test: String): Creator = {

    val creator: Option[Creator] = test match {
      case CreatorFactory.`testCase1` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_1.Creator())
      case CreatorFactory.`testCase2` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_2.Creator())
      case CreatorFactory.`testCase3` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_3.Creator())
      case CreatorFactory.`testCase4` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_4.Creator())
      case CreatorFactory.`testCase5` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_5.Creator())
      case CreatorFactory.`testCase7` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_7.Creator())
      case CreatorFactory.`testCasea1` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_a1.Creator())
      case CreatorFactory.`testCasea2` => Some(it.au.misure.calcolo_capacita.utility.test_case.test_case_a2.Creator())

      case _ =>
        None
    }

    if (!creator.isDefined) {
      println("error with test mock")
      System.exit(1)
    }
    creator.get

  }
}
