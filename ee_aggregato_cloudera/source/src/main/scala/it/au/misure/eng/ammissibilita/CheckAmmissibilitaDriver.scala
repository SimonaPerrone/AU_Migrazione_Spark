package it.au.misure.eng.ammissibilita

import it.au.misure.eng.ammissibilita.file.CheckAmmissibilitaFile
import it.au.misure.eng.ammissibilita.pod.CheckAmmissibilitaPod
import it.au.misure.util.LoggingSupport

object CheckAmmissibilitaDriver extends LoggingSupport {

  def run(args: Array[String]): Unit = {

    log.info("Starting File Validation")
    val admissibleFiles = CheckAmmissibilitaFile.run(args)
    log.info("Ended File Validation")

    log.info("Starting POD Validation")
    CheckAmmissibilitaPod.run(args, admissibleFiles)
    log.info("Ended POD Validation, Exiting...")

  }

}
