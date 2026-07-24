package it.au.misure.eng.utility

object SystemUtility {
  def setLocalLaunch(): Unit = {
    System.setProperty("LOCAL_LAUNCH", "true")
  }

  def isLocalLaunch :Boolean = {
    System.getProperty("LOCAL_LAUNCH", "false").toBoolean
  }
}
