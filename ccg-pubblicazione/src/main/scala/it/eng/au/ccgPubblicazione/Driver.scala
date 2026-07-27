package it.eng.au.ccgPubblicazione

import it.eng.au.ccgPubblicazione.args.FlowArgsFactory
import it.eng.au.ccgPubblicazione.controller.sessionfactory.impl.{AggSession, CdpFinSession, CdpRicSession, SbgSession}
import it.eng.au.ccgPubblicazione.utility.Constants._
import org.apache.log4j.Logger

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      logger.warn(s"Start Pubblicazione Contatore Consumi Gas")

      // Parsing delle properties
      val flowArgsConfig = FlowArgsFactory.parse(args)

      // Esecuzione del processo in base alla sessione
      flowArgsConfig.session match {
        case AGG => AggSession.run(flowArgsConfig)
        case SBG => SbgSession.run(flowArgsConfig)
        case CDP_FIN => CdpFinSession.run(flowArgsConfig)
        case CDP_RIC => CdpRicSession.run(flowArgsConfig)
      }
    }
    catch {
      case e: Exception => logger.error(e.getStackTrace); throw e
      case e: Error => logger.error(e.getStackTrace); throw e
    }
  }
}
