package it.au.misure.eng.ammissibilita.pod

import it.au.misure.cli.FlussoMisureTool
import it.au.misure.eng.utility.SystemUtility
import junit.framework.TestCase
import org.junit.Ignore

@Ignore
class TestCheckAmmissibilitaPod extends TestCase {
  SystemUtility.setLocalLaunch()

  def testRun(): Unit = {
    SystemUtility.setLocalLaunch()
    val arguments: Array[String] = Array(
      "-iap",
      "-g",
      "-y", "2019",
      "-m", "09",
      "-s", "16"
    )

    FlussoMisureTool.main(arguments)
  }
}
