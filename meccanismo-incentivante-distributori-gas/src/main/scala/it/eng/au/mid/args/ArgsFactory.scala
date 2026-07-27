package it.eng.au.mid.args

import org.apache.log4j.Logger
import scopt.OptionParser

import java.time.LocalDate

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  val flowOptions: List[String] = List(Args.SBG_FLOW, Args.AGG_FLOW, Args.AGG_BIT_FLOW, Args.MID1_PREP, Args.MID1_PUBB, Args.MID2_PREP, Args.MID2_PUBB)

  def parse(args: Array[String]): Args = {
    val parser: OptionParser[Args] = new OptionParser[Args]("MID") {
      head("Meccanismo incentivante distributori gas")

      opt[String]('p', "properties")
        .required()
        .action((x, c) => c.copy(pathToProperties = x))
        .text("Percorso alle properties (hdfs)")

      opt[String]('f', "flow")
        .required()
        .action((x, c) => c.copy(flow = x.toUpperCase()))
        .validate(x =>
          if (flowOptions.contains(x.toUpperCase())) success
          else failure(s"Errore, i valori accettati sono: ${flowOptions.mkString("/")}"))
        .text(s"Tipo di processo da lanciare: ${flowOptions.mkString("/")}")

      help('h', "help") text "Stampa le opzioni"
    }

    parser.parse(args, Args()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

}
