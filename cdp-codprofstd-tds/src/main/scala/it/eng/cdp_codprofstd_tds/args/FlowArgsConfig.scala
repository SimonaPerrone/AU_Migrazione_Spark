package it.eng.cdp_codprofstd_tds.args

case class FlowArgsConfig(propertiesPath: String = null) {
  override def toString: String =
    s"FlowArgsConfig=[ " +
      s"propertiesPath=$propertiesPath" +
      s"]"
}