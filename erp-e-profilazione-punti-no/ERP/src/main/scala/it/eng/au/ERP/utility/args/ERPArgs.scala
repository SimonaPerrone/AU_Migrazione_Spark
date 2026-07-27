package it.eng.au.ERP.utility.args

import it.eng.au.ERP.utility.functions.Constants
import org.apache.log4j.Logger
import scopt.OptionParser

object ERPArgs extends Args[ERPArgsConfig] {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  val validValuesFlow = Set(
    Constants.flowTERNA,
    Constants.flowIP,
    Constants.flowDIST,
    Constants.flowINT,
    Constants.flowFULL,
    Constants.flowNO,
    Constants.flowSALDO,
    Constants.flowDATI
  )

  override def parse(args: Array[String]): ERPArgsConfig = {

    val parser = new OptionParser[ERPArgsConfig]("erp-e-proliferazione-punti-no") {
      head("erp-e-proliferazione-punti-no")

      opt[String]("calcolo")
        .abbr("c")
        .optional()
        .valueName("<value>")
        .validate { value =>
          if (validValuesFlow.contains(value)) success
          else failure(s"Invalid value '$value' for -calcolo. Valid values are: ${validValuesFlow.mkString(", ")}")
        }
        .action((x, c) => c.copy(flow = Some(x)))


      opt[String]( "properties")
        .abbr("p")
        .required()
        .action((x, c) => c.copy(pathToProperties = x))
        .text("Percorso al path hdfs properties")

      // New required options
//      opt[Int]("annomese")
//        .abbr("am")
//        .required()
//        .validate { x =>
//          val year = x / 100
//          val month = x % 100
//          if (x.toString.length != 6)
//            failure("AnnoMese must be exactly 6 digits in format YYYYMM.")
//          else if (month < 1 || month > 12)
//            failure("Month in AnnoMese must be between 01 and 12.")
//          else
//            success
//        }
//        .action((x, c) => c.copy(annomese = Some(x)))
//        .text("AnnoMese in formato YYYYMM")

//      opt[String]("singola_piva_distr")
//        .abbr("pd")
//        .required()
//        .action((x, c) => c.copy(singola_piva_distributore = Some(x)))
//        .text("Singola PIVA distributore")

      opt[String]("path_pod_esclusi")
        .abbr("pe")
        .required()
        .action((x, c) => c.copy(path_esclusione_pod = x))
        .text("Path file esclusione POD")

      opt[String]("DATI")
        .abbr("d")
        .optional()
        .action((x, c) => c.copy(dati = Some(x)))
        .text("Percorso base output per messa a disposizione DATI (HDFS o FS)")

//      opt[String]("area")
//        .abbr("a")
//        .required()
//        .action((x, c) => c.copy(area = Some(x)))
//        .text("Area")

    }


    parser.parse(args, ERPArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }
}
