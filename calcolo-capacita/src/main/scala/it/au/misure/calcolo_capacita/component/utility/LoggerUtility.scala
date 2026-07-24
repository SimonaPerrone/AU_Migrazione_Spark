package it.au.misure.calcolo_capacita.component.utility

import org.apache.log4j.Logger

object LoggerUtility {

  def printInfo(message: String, className:String):Unit={
    Logger.getLogger(className).info(message)
  }
  def printError(message: String, className:String):Unit={
    Logger.getLogger(className).error(message)
  }
}
