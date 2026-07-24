package it.eng.au.pubblicazionePcg.args

import org.apache.log4j.{LogManager, Logger}
import scopt.OptionParser

object ParseCLIArgs {
  @transient val logger: Logger = LogManager.getLogger(this.getClass)

  def parse(args: Array[String]): CLIArgsConfig = {

    val parser = new OptionParser[CLIArgsConfig]("[CLG] Pubblicazione PCG") {
      head("[CLG] Pubblicazione PCG")

      opt[String]('m', "offset-mese") action { (x, c) =>
        c.copy(offestMese = Option(x))
      } text "Month difference with respect to current month. The process will import measures from {current_month - offset-mese}."

      opt[String]('s', "sbg-type") action { (x, c) =>
        c.copy(sbgType = Option(x))
      } text "SBG type."

      opt[String]('n', "num-lines-per-csv") action { (x, c) =>
        c.copy(numLinesPerCsv = Option(x))
      } text "Number or lines per CSV. A value of 110000 should produce CSV files of size ~23MB."

      opt[String]('d', "daily-consumption-table-name") action { (x, c) =>
        c.copy(dailyConsumptionTableName = Option(x))
      } text "Name of the table where the SBG measures are stored."

      opt[String]('o', "isilon-basepath-out") action { (x, c) =>
        c.copy(isilonBasepathOut = Option(x))
      } text "Output path in which CSV files are created."

      opt[String]('l', "hdfs-output-basepath-info-log") action { (x, c) =>
        c.copy(hdfsOutputBasepathInfoLog = Option(x))
      } text "Path of the table in which the publication log is saved."

      opt[String]('i', "sbgmisure-hdfs-path") action { (x, c) =>
        c.copy(sbgMisureHdfsPath = Option(x))
      } text "Path of the table where the import process stores the measures."

      help('h', "help") text "prints all options"
    }

    val cliArgs = parser.parse(args, CLIArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }

    checkArgs(cliArgs)

    cliArgs
  }

  def checkArgs(argsConfig: CLIArgsConfig): Unit = {
    if (argsConfig.offestMese.isEmpty)
      throw new IllegalArgumentException(s"Error while parsing offset-mese.")
    if (argsConfig.numLinesPerCsv.isEmpty)
      throw new IllegalArgumentException(s"Error while parsing num-lines-per-csv.")
    if (argsConfig.sbgType.isEmpty)
      throw new IllegalArgumentException(s"Error while parsing sbg-type.")
    if (argsConfig.dailyConsumptionTableName.isEmpty)
      throw new IllegalArgumentException(s"Error while parsing sbg-misure-hdfs-path.")
    if (argsConfig.isilonBasepathOut.isEmpty)
      throw new IllegalArgumentException(s"Error while parsing isilon-basebath-out.")
    if (argsConfig.hdfsOutputBasepathInfoLog.isEmpty)
      throw new IllegalArgumentException(s"Error while parsing hdfs-output-basepath-info-log.")
  }
}

