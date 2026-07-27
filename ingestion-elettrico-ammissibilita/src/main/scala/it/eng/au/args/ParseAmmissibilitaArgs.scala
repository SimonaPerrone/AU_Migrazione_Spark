package it.eng.au.args

import it.eng.au.utility.Constants
import org.apache.log4j.Logger
import scopt.OptionParser

object ParseAmmissibilitaArgs {

  @transient val log: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): AmmissibilitaParameters = {
    val parser = new OptionParser[AmmissibilitaParameters]("ingestion-elettrico-ammissibilita") {
      head("ingestion-elettrico-ammissibilita")

      // option -y, --anno
      opt[String]('y', "anno") action {(x, c) =>
        c.copy(year = x)
      } text("Anno di riferimento")   required()

      opt[String]('m', "mese") action {(x, c) =>
        c.copy(month = x)
      } text("Mese di riferimento")   required()

      opt[String]('d', "giorno") action {(x, c) =>
        c.copy(day = x)
      } text("Giorno di riferimento")  required()

      opt[Unit]('g', "1g") action { (x, c) =>
        c.copy(g = Constants._1G)
      } text "Specifica l'ingestione dei flussi 1G"

      opt[Unit]('G', "2g") action { (x, c) =>
        c.copy(g = Constants._2G)
      } text "Specifica l'ingestione dei flussi 2G"

      opt[Unit]('S', "smis") action { (x, c) =>
        c.copy(isSmis = true)
      } text "Acquisisce flussi smis"

      help('h', "help") text "prints all options"

      checkConfig(
        c =>
          if (c.g == "") {
            log.error("**** Bisogna specificare se 1G/2G")
            throw new IllegalArgumentException("**** Bisogna specificare se 1G/2G")
          } else success )
    }

    parser.parse(args, AmmissibilitaParameters()) match {
      case Some(c) => c
      case None =>
        log.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }


}
