package it.eng.au.portale_consumi_ee.utility.args

import it.eng.au.portale_consumi_ee.common.utility.args.Args
import org.apache.log4j.Logger
import scopt.OptionParser
//todo set up properly in function of storic flow necessity
object MisureEEArgs extends Args[MisureEEArgsConfig] {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  val validValuesPCMS = Set( "33M", "3M")


  override def parse(args: Array[String]): MisureEEArgsConfig = {

    val parser = new OptionParser[MisureEEArgsConfig]("portale-consumi-ee misure storico") {
      head("portale-consumi-ee misure storico")

      opt[String]("PC_MS")
        .required()
        .valueName("<value>")
        .validate { value =>
          if (validValuesPCMS.contains(value)) success
          else failure(s"Invalid value '$value' for -PC_MS. Valid values are: ${validValuesPCMS.mkString(", ")}")
        }
        .action((value, config) => {
          config.copy(
            flow = if (value == "33M") "full" else "partial",
            storico = if (value == "33M") true else false
          )
        })
        .text(s"PC_MS must be one of: ${validValuesPCMS.mkString(", ")}")

      opt[String]('p', "properties")
        .required()
        .action((x, c) => c.copy(pathToProperties = x))
        .text("Percorso al path hdfs properties")
    }

    parser.parse(args, MisureEEArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

}
