package it.eng.au.gse.calcoloMensile

import it.eng.au.gse.calcoloMensile.args.ArgsFactory
import it.eng.au.gse.calcoloMensile.controller.{Prepare, Transform}
import it.eng.au.gse.calcoloMensile.dao.{DwhConsumiDao, GseAggrMDao, GseRichiesteMensiliDao}
import it.eng.au.gse.calcoloMensile.utility.environment.EnvironmentInit
import it.eng.au.gse.common.controller.{ConsumptionController, PrepareCommon, TransformCommon}
import it.eng.au.gse.common.dao.GsePerimetroDao
import it.eng.au.gse.common.utility.LogUtility
import it.eng.au.gse.common.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel

import java.time.format.DateTimeFormatter

object Driver {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = ArgsFactory.parse(args)

      val applicationName = "[GSE] Energy Release - Calcolo Mensile"
      val logName = "GSE LOG:"

      EnvironmentInit.setEnvironment(applicationName, logName, parsedArgs)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()
    } catch {
      case e: Throwable =>
        logger.error(s"An error occurred in the procedure.")
        throw e
    }
  }

  def run(): Unit = {
    val executionId = Environment.executionId
    val startDate = Environment.startDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val gseRichiestaDao = new GseRichiesteMensiliDao()
    val gsePerimetroDao = new GsePerimetroDao()
    val dwhConsumiDao = new DwhConsumiDao()
    val gseAggrMDao = new GseAggrMDao()

    val requestsDf = gseRichiestaDao.readTable
    val perimeterDf = gsePerimetroDao.readTable
    val dwhConsumiDf = dwhConsumiDao.readTable

    val podPerimeter = Prepare.preparePodPerimeter(perimeterDf)
    val (newRequests, yearMonthList) = Prepare.prepareRequests(requestsDf)
    val dwhConsumi = PrepareCommon.prepareDwhConsumi(dwhConsumiDf, yearMonthList)
    val (requestsWithinPerimeter, pods) = Transform.joinPerimeterAndRequests(podPerimeter, newRequests)

    pods.persist(StorageLevel.MEMORY_AND_DISK)
    logger.warn(s"Numero di coppie (pod, annomese) di cui effettuare il calcolo: ${pods.count}")

    val dwhConsumiFiltered = TransformCommon.joinDwhConsumiWithPods(dwhConsumi, pods)
    val monthlyConsumptions = ConsumptionController.computeMonthlyConsumptions(dwhConsumiFiltered)
    val gseAggrM = Transform.joinRequestsAndConsumptions(requestsWithinPerimeter, monthlyConsumptions, startDate, executionId)
    gseAggrM.persist(StorageLevel.MEMORY_AND_DISK)

    gseAggrMDao.write(gseAggrM)
    gseAggrMDao.writeExport(gseAggrM)
  }
}
