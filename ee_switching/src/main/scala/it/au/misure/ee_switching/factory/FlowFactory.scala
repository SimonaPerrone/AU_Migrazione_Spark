package it.au.misure.ee_switching.factory

import it.au.misure.ee_switching.flow._
import it.au.misure.ee_switching.utility.Constants.{FUNZIONALI, STORICI}

object FlowFactory {
  def getFlow(flowName: String): Option[Flow] = {
    flowName match {
      case FUNZIONALI => Some(FunzionaliFlow)
      case STORICI => Some(StoriciFlow)
      case _ => None
    }
  }
}
