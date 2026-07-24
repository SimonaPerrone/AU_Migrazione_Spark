package it.eng.au.pubblicazione_cce.args

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import org.apache.log4j.Logger
import scopt.OptionParser

import java.time.LocalDate

object ArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  private val processi: List[String] = List(CostantiCCE.PROCESSO_CA, CostantiCCE.PROCESSO_P, CostantiCCE.PROCESSO_PEIN.toUpperCase() ,
    CostantiCCE.PROCESSO_PR, CostantiCCE.PROCESSO_PREIN.toUpperCase() ).sorted

  def parse(args: Array[String]): Args = {
    val parser: OptionParser[Args] = new OptionParser[Args]("PubblicazioneCCE") {
      head("Pubblicazione CCE")

      opt[String]('p', "properties")
        .required()
        .action((x, c) => c.copy(pathToProperties = x))
        .text("Percorso alle properties (hdfs)")

      opt[String]('d', "data")
        .optional()
        .withFallback(() => LocalDate.now().minusDays(1).toString)
        .action((x, c) => try {
          c.copy(dataRichieste = LocalDate.parse(x))
        }
        catch {
          case _: Exception => throw new IllegalArgumentException(s"Errore, il formato della data deve essere YYYY-MM-DD")
        }
        )
        .text(s"(Opzionale) Data richieste da eseguire (se non impostato allora valorizzato con giorno precedente a lancio)")


      opt[String]('f', "flow")
        .optional()
        .action((x, c) => c.copy(flow = x.toUpperCase))
        .validate(x =>
          if (processi.contains(x.toUpperCase())) success
          else failure(s"Errore, i valori accettati sono: ${processi.mkString("/")}")
        )
        .text(s"Lancia un processo specifico: ${processi.mkString("/")}")

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
