package it.eng.au.portale_consumi_ee.utility.args

import it.eng.au.portale_consumi_ee.common.utility.args.Args
import org.apache.log4j.Logger
import scopt.OptionParser

object MisureEEArgs extends Args[MisureEEArgsConfig] {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  val validValuesPCEX = Set("FULL-SPLIT3", "IS-SPLIT3", "FULL-SPLIT33", "IS-SPLIT33")
//  Set("ML-SPLIT3", "M-SPLIT3", "ML-SPLIT33", "M-SPLIT33")

  //full -> FULL
  //InizializationStage -> IS

  override def parse(args: Array[String]): MisureEEArgsConfig = {

    val parser = new OptionParser[MisureEEArgsConfig]("portale-consumi-ee ExportMongo") {
      head("portale-consumi-ee ExportMongo")

      opt[String]("PC_EX")
        .required()
        .valueName("<value>")
        .validate { value =>
          if (validValuesPCEX.contains(value)) success
          else failure(s"Invalid value '$value' for -PC_EX. Valid values are: ${validValuesPCEX.mkString(", ")}")
        }
        .action((value, config) => {
          val arguments = value.split("-")
          val phase = arguments(0)
          val window = arguments(1)
          config.copy(
            flow = if (phase == "FULL") "EXPORT" else "INIT",
            storico = if (window == "SPLIT3") false else true
          )
        })
        .text(s"PC_EX must be one of: ${validValuesPCEX.mkString(", ")}")

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
