package it.eng.au.pubblicazioneRendiconti

import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import it.eng.au.indennizziMisureGasCommon.utility.log.LogUtility
import it.eng.au.pubblicazioneRendiconti.args.RendicontiArgsFactory
import it.eng.au.pubblicazioneRendiconti.controller.PubblicazioneIndennizzi
import it.eng.au.pubblicazioneRendiconti.dao.{IndennizziRzg2DAO, ReportPubblicazioneRzg2DAO}
import it.eng.au.pubblicazioneRendiconti.utility.PubblicazioneRendicontiEnvironment
import it.eng.au.pubblicazioneRendiconti.utility.properties.Properties
import org.apache.log4j.Logger

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val parsedArgs = RendicontiArgsFactory.parse(args)

      PubblicazioneRendicontiEnvironment.setEnvironment(parsedArgs)

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
    val indennizziRzg2DAO = new IndennizziRzg2DAO()
    val reportPubblicazioneRzg2DAO = new ReportPubblicazioneRzg2DAO()

    val (indennizziRzg2, partitionRead) = if (!Properties.isRecoveryMode)
      indennizziRzg2DAO.readLastPartition()
    else indennizziRzg2DAO.readTableByPartiton(Properties.getCigIndennizziRzg2ExecutionId)

    val reportPubblicazioneRzg2RDD = PubblicazioneIndennizzi.getAndWriteIndennizzi(indennizziRzg2, partitionRead)

    val reportPubblicazioneRzg2 = reportPubblicazioneRzg2DAO.get(reportPubblicazioneRzg2RDD)
    reportPubblicazioneRzg2DAO.write(reportPubblicazioneRzg2)
  }
}
