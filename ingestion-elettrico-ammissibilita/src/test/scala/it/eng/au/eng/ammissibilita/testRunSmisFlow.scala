package it.eng.au.eng.ammissibilita

//import it.au.misure.cli.FlussoMisureTool
import it.eng.au.ammissibilita.pod.CheckAmmissibilitaPod
import it.eng.au.model.ReportEsitoPODMessage
import it.eng.au.utility.{Constants, SystemUtility}
import junit.framework.TestCase
import org.junit.{Assert, Ignore}

@Ignore // There is no test here so, in order to avoid the exception "No tests found in file..", we put Ignore.
class testRunSmisFlow extends TestCase{
  SystemUtility.setLocalLaunch()

  /*
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
  }*/

}
