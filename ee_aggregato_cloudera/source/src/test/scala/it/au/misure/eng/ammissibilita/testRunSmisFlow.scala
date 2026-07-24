package it.au.misure.eng.ammissibilita

import it.au.misure.cli.FlussoMisureTool
import it.au.misure.eng.ammissibilita.pod.CheckAmmissibilitaPod
import it.au.misure.eng.model.ReportEsitoPODMessage
import it.au.misure.eng.utility.{Constants, SystemUtility}
import junit.framework.TestCase
import org.junit.{Assert, Ignore}

class testRunSmisFlow extends TestCase{
  SystemUtility.setLocalLaunch()

  @Ignore
  def testRun(): Unit = {
    val arguments: Array[String] = Array(
      "-ia",
      "-g",
      "-SS",
      "-y", "2018",
      "-m", "12",
      "-s", "03"
    )

    FlussoMisureTool.main(arguments)
  }

}
