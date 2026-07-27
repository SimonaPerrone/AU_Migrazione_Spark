package it.eng.au.ammissibilita

import it.eng.au.ammissibilita.file.CheckAmmissibilitaFile
import it.eng.au.ammissibilita.pod.CheckAmmissibilitaPod
import it.eng.au.ammissibilita.pod.CheckAmmissibilitaPod.parseAmmissibilitaArgs
import it.eng.au.utility.environment.Environment
import it.eng.au.utility.{CheckSqoopUtility, PropertyUtility}
import org.apache.log4j.Logger //import it.au.misure.util.LoggingSupport

object CheckAmmissibilitaDriver { //extends LoggingSupport {

  @transient val log: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val params = parseAmmissibilitaArgs(args)

    if (params.isSmis) Environment.getOrCreate("Ammissibiità SMIS") else Environment.getOrCreate("Ammissibiità EE")

    if (!PropertyUtility.getCheckSqoop || (
      CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuAziendaPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuDistrPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuEmtPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuPodDistrPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuPodPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuPodUddPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcusPodDistrPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcusPodPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcusPodUddPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcusUddPTable) &&
        CheckSqoopUtility.checkSqoopDateIsToday(PropertyUtility.getRcuUddPTable)
    )) {
      if (PropertyUtility.getCheckSqoop) {
        log.warn("check sqoop rcu table is successful.")
      } else log.warn("check sqoop functionality is disabled")

      log.warn("Starting File Validation")
      val admissibleFiles = CheckAmmissibilitaFile.run(args)
      log.warn("Ended File Validation")

      log.warn("Starting POD Validation")
      CheckAmmissibilitaPod.run(args, admissibleFiles)
      log.warn("Ended POD Validation, Exiting...")
    } else {
      log.error("One of the rcu/rcugas tables has not been updated today")
      throw new Exception(s"One of the rcu/rcugas tables has not been updated today")
    }

  }

}
