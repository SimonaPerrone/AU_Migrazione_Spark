package it.eng.au.aggiustamentoGas.utility.args

import org.apache.log4j.Logger
import org.joda.time.DateTime
import scopt.OptionParser

import scala.util.Try

object FlowArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  /**
   * Effettua il parsing dei parametri in input al processo. In particolare,
   *
   *  - `-p` è il percorso su HDFS da cui leggere il file di configurazione;
   *  - `-s` è la sessione di calcolo (AGGiustamento, Contatore Consumi Gas, Sessione Bilanciamento Gas)
   *  - `-d` è la data utilizzata per il processo di CCG
   *
   * @param `args` un array di stringhe
   * @return un'istanza di `FlowArgsConfig`, contenente i valori passati come input al processo
   */
  def parse(args: Array[String]): FlowArgsConfig = {
    val parser = new OptionParser[FlowArgsConfig]("aggiustamento-gas") {
      head("aggiustamento-gas")

      opt[String]('p', "properties") action { (x, c) =>
        c.copy(pathToProperties = x)
      } text "Path to properties" required()

      opt[String]('s', "session") action { (x, c) =>
        val upperValue = x.toUpperCase()
        if (!List("AGG", "CCG", "SBG").contains(upperValue)) throw new IllegalArgumentException(s"Error values accepted are AGG/CCG/SBG")
        c.copy(session = upperValue)
      } text "Process type to run: AGG/SBG/CCG"

      opt[String]('d', "date") action { (x, c) =>
        c.copy(dateToRun = Try(new DateTime(x)).toOption)
      } text "Date to run. Format yyyy-MM-dd. For AGG session this is ignored"

      help('h', "help") text "prints all options"
    }

    parser.parse(args, FlowArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

}
