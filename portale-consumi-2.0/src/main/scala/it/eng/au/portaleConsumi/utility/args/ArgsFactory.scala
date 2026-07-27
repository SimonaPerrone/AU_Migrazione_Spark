package it.eng.au.portaleConsumi.utility.args

import org.apache.log4j.Logger
import scopt.OptionParser

import java.time.LocalDate

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  val flowOptions: Seq[String] = PortaleConsumiArgs.flowsOptions
  val intervalOptions: Seq[String] = PortaleConsumiArgs.intervalOptions

  def parse(args: Array[String]): PortaleConsumiArgs = {
    val parser: OptionParser[PortaleConsumiArgs] = new OptionParser[PortaleConsumiArgs]("Portale consumi gas") {
      head("Portale consumi gas")

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

      opt[String]('i', "interval")
        .action((x, c) => c.copy(interval = x.toUpperCase()))
        .validate(x =>
          if (intervalOptions.contains(x.toUpperCase())) success
          else failure(s"Errore, i valori accettati sono: ${intervalOptions.mkString("/")}")
        )
        .text(s"Tipo di processo: ${intervalOptions.mkString("/")}")

      opt[String]('d', "runDay")
        .required()
        .withFallback(() => LocalDate.now().toString)
        .action((x, c) => try {
          c.copy(runDay = LocalDate.parse(x))
        }
        catch {
          case _: Exception => throw new IllegalArgumentException(s"Errore, il formato della data deve essere YYYY-MM-DD")
        }
        )
        .text(s"(Opzionale) Giorno di avvio processo usato per il calcolo dell'intervallo (se nullo allora data odierna)")

      opt[Int]('x', "dayInterval")
        .optional()
        .action((x, c) => c.copy(dayInterval = x)
        ) text s"(Opzionale) Numero giorni nel passato da cui iniziare il calcolo"

      opt[Boolean]('s', "skipMisure")
        .optional()
        .action((x, c) => c.copy(skipMisure = x)
        ) text s"Salta aggiornamento misure"

      help('h', "help") text "Stampa le opzioni"
    }

    parser.parse(args, PortaleConsumiArgs()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

}
