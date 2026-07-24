package it.eng.au.pubblicazionePcg.args

case class CLIArgsConfig(
                          offestMese: Option[String] = None,
                          numLinesPerCsv: Option[String] = None,
                          sbgType: Option[String] = None,
                          sbgMisureHdfsPath: Option[String] = None,
                          dailyConsumptionTableName: Option[String] = None,
                          isilonBasepathOut: Option[String] = None,
                          hdfsOutputBasepathInfoLog: Option[String] = None
                        ) {
  override def toString: String =
    s"FlowArgsConfig=[ " +
      s"offestMese=${offestMese.getOrElse("")}" +
      s"numLinesPerCsv=${numLinesPerCsv.getOrElse("")}" +
      s"sbgType=${sbgType.getOrElse("")}" +
      s"dailyConsumptionTableName=${dailyConsumptionTableName.getOrElse("")}" +
      s"sbgMisureHdfsPath=${sbgMisureHdfsPath.getOrElse("")}" +
      s"isilonBasepathOut=${isilonBasepathOut.getOrElse("")}" +
      s"hdfsOutputBasepathInfoLog=${hdfsOutputBasepathInfoLog.getOrElse("")}" +
      s" ]"

}