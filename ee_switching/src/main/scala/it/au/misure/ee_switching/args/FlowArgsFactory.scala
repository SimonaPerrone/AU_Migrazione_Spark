package it.au.misure.ee_switching.args

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import it.au.misure.ee_switching.utility.Constants.{INPUT_TIMESTAMP_PATTERN, STORICI}
import it.au.misure.ee_switching.utility.FileUtility
import org.apache.log4j.Logger
import scopt.OptionParser

object FlowArgsFactory {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)
  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)

  def parse(args: Array[String]): FlowArgsMetadata = {
    val parser = new OptionParser[FlowArgsMetadata]("ee-switching") {
      head("ee-switching")

      opt[String]('n', "flow-name") action { (x, c) =>
        c.copy(flowName = x)
      } text "nome tipologia flusso SW" required()

      opt[String]('t', "timestamp") action { (x, c) =>
        c.copy(timestampFilePath = x, runOrdinaria = false)
      } text "filepath contenente il timestamp di creazione dei dati in ambiente Hadoop"

      opt[String]('p', "pod") action { (x, c) =>
        c.copy(listaPodFilePath = x)
      } text "filepath lista di Pods"

      opt[String]('d', "piva-distributore") action { (x, c) =>
        c.copy(listaDistributoriFilePath = x)
      } text "filepath lista di PIVA di Distributori"

      opt[String]('D', "piva-udd") action { (x, c) =>
        c.copy(listaUddFilePath = x)
      } text "filepath lista di PIVA di UDD"

      opt[String]('s', "date-storici-switching") action { (x, c) =>
        c.copy(listaDateStoriciSWFilePath = x, runOrdinaria = false)
      } text "filepath lista di date di switching per flussi storici"

      opt[String]('f', "date-funzionali-switching") action { (x, c) =>
        c.copy(listaDateFunzionaliSWFilePath = x, runOrdinaria = false)
      } text "filepath lista di date di switching per flussi funzionali"

      opt[String]('F', "date-funzionali-na") action { (x, c) =>
        c.copy(listaDateFunzionaliNAFilePath = x, runOrdinaria = false)
      } text "filepath lista di date di nuova attivazione per flussi funzionali"

      opt[String]('c', "coppie-piva") action { (x, c) =>
        c.copy(listaCoppieDistrUddFilePath = x)
      } text "filepath lista di coppie PIVA Distributore,UDD"

      opt[String]('q', "queue") action { (x, c) =>
        c.copy(queue = x)
      } text "Used to select dynamic resource pool for spark job"

      help('h', "help") text "prints all options"
    }
    parser.parse(args, FlowArgsMetadata()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }

  def getInputArgs(flowArgsMeta: FlowArgsMetadata): FlowArgsConfig = {

    var argsConfig: FlowArgsConfig = FlowArgsConfig(flowName = flowArgsMeta.flowName, runOrdinaria = flowArgsMeta.runOrdinaria)

    if (flowArgsMeta.timestampFilePath.nonEmpty) {
      val timestamp = FileUtility.readTextFile(flowArgsMeta.timestampFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty)
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con il timestamp (${flowArgsMeta.timestampFilePath})")
      }
      if (timestamp.length != 1)
        throw new IllegalArgumentException(s"Un solo timestamp deve essere specificato dentro al file ${flowArgsMeta.timestampFilePath}")
      var timestampValue: Timestamp = null
      try {
        timestampValue = Timestamp.valueOf(LocalDateTime.parse(timestamp.head, DateTimeFormatter.ofPattern(INPUT_TIMESTAMP_PATTERN)))
      } catch {
        case e: Exception => throw new IllegalArgumentException(s"Fallito parsing del timestamp (${timestamp.head}) dentro al file ${flowArgsMeta.listaDateFunzionaliSWFilePath} (${e.getMessage}). Formato richiesto: yyyy-MM-dd HH:mm:ss")
      }
      if (timestampValue == null)
        throw new IllegalArgumentException(s"Fallito parsing del timestamp (${timestamp.head}) dentro al file ${flowArgsMeta.listaDateFunzionaliSWFilePath}. Formato richiesto: yyyy-MM-dd HH:mm:ss")

      argsConfig = argsConfig.copy(timestamp = timestampValue)
    }

    if (flowArgsMeta.listaPodFilePath.nonEmpty) {
      val listaPod = FileUtility.readTextFile(flowArgsMeta.listaPodFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty).distinct
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con la lista di pod (${flowArgsMeta.listaPodFilePath})")
      }
      if (listaPod.isEmpty)
        throw new IllegalArgumentException(s"Nessun pod specificato dentro al file ${flowArgsMeta.listaPodFilePath}")
      argsConfig = argsConfig.copy(listaPod = listaPod)
    }

    if (flowArgsMeta.listaDistributoriFilePath.nonEmpty) {
      val listaDistributori = FileUtility.readTextFile(flowArgsMeta.listaDistributoriFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty).distinct
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con la lista di distributori (${flowArgsMeta.listaDistributoriFilePath})")
      }
      if (listaDistributori.isEmpty)
        throw new IllegalArgumentException(s"Nessun distributore specificato dentro al file ${flowArgsMeta.listaDistributoriFilePath}")
      argsConfig = argsConfig.copy(listaDistributori = listaDistributori)
    }

    if (flowArgsMeta.listaUddFilePath.nonEmpty) {
      val listaUdd = FileUtility.readTextFile(flowArgsMeta.listaUddFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty).distinct
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con la lista di udd (${flowArgsMeta.listaUddFilePath})")
      }
      if (listaUdd.isEmpty)
        throw new IllegalArgumentException(s"Nessun udd specificato dentro al file ${flowArgsMeta.listaUddFilePath}")
      argsConfig = argsConfig.copy(listaUdd = listaUdd)
    }

    if (flowArgsMeta.listaDateFunzionaliSWFilePath.nonEmpty) {
      val listaDateFunzionaliSW = FileUtility.readTextFile(flowArgsMeta.listaDateFunzionaliSWFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty).distinct
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con la lista di date di switching per i flussi funzionali (${flowArgsMeta.listaDateFunzionaliSWFilePath})")
      }

      var listaDateFunzionaliSWFormatted: List[LocalDate] = null
      try {
        listaDateFunzionaliSWFormatted = listaDateFunzionaliSW.map(date => LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE))
      } catch {
        case e: Exception => throw new IllegalArgumentException(s"Fallito parsing delle date dentro al file ${flowArgsMeta.listaDateFunzionaliSWFilePath} (${e.getMessage})")
      }
      if (listaDateFunzionaliSWFormatted == null || listaDateFunzionaliSWFormatted.isEmpty)
        throw new IllegalArgumentException(s"Nessuna data di switching per i flussi funzionali specificata dentro al file ${flowArgsMeta.listaDateFunzionaliSWFilePath}")

      argsConfig = argsConfig.copy(listaDateSW = listaDateFunzionaliSWFormatted)
    }

    if (flowArgsMeta.listaDateFunzionaliNAFilePath.nonEmpty) {
      val listaDateFunzionaliNA = FileUtility.readTextFile(flowArgsMeta.listaDateFunzionaliNAFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty).distinct
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con la lista date di nuova attivazione per i flussi funzionali (${flowArgsMeta.listaDateFunzionaliNAFilePath})")
      }

      var listaDateFunzionaliNAFormatted: List[LocalDate] = null
      try {
        listaDateFunzionaliNAFormatted = listaDateFunzionaliNA.map(date => LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE))
      } catch {
        case e: Exception => throw new IllegalArgumentException(s"Fallito parsing delle date dentro al file ${flowArgsMeta.listaDateFunzionaliNAFilePath} (${e.getMessage})")
      }
      if (listaDateFunzionaliNAFormatted == null || listaDateFunzionaliNAFormatted.isEmpty)
        throw new IllegalArgumentException(s"Nessuna data di nuova attivazione per i flussi funzionali specificata dentro al file ${flowArgsMeta.listaDateFunzionaliNAFilePath}")

      argsConfig = argsConfig.copy(listaDateNA = listaDateFunzionaliNAFormatted)
    }

    if (flowArgsMeta.listaDateStoriciSWFilePath.nonEmpty) {
      val listaDateStoriciSW = FileUtility.readTextFile(flowArgsMeta.listaDateStoriciSWFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty).distinct
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con la lista di date di switching per i flussi storici (${flowArgsMeta.listaDateStoriciSWFilePath})")
      }

      var listaDateStoriciSWormatted: List[LocalDate] = null
      try {
        listaDateStoriciSWormatted = listaDateStoriciSW.map(date => LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE))
      } catch {
        case e: Exception => throw new IllegalArgumentException(s"Fallito parsing delle date dentro al file ${flowArgsMeta.listaDateStoriciSWFilePath} (${e.getMessage})")
      }
      if (listaDateStoriciSWormatted == null || listaDateStoriciSWormatted.isEmpty)
        throw new IllegalArgumentException(s"Nessuna data di switching per i flussi storici specificata dentro al file ${flowArgsMeta.listaDateStoriciSWFilePath}")

      argsConfig = argsConfig.copy(listaDateSW = listaDateStoriciSWormatted.sorted)
    }

    if (flowArgsMeta.listaCoppieDistrUddFilePath.nonEmpty) {
      val listaCoppieDistrUdd = FileUtility.readTextFile(flowArgsMeta.listaCoppieDistrUddFilePath) match {
        case Some(lines) => lines.map(_.trim).filter(_.nonEmpty).map(line => line.split(",")).filter(_.length == 2).map(coppia => (coppia.head.trim, coppia.last.trim)).distinct
        case None => throw new IllegalArgumentException(s"Impossibile aprire il file con la lista di coppie distributore,udd (${flowArgsMeta.listaCoppieDistrUddFilePath})")
      }
      if (listaCoppieDistrUdd.isEmpty)
        throw new IllegalArgumentException(s"Nessuna coppia distributore,udd specificata dentro al file ${flowArgsMeta.listaCoppieDistrUddFilePath}")
      argsConfig = argsConfig.copy(listaCoppieDistrUdd = listaCoppieDistrUdd)
    }

    argsConfig
  }

  def checkInputArgs(params: FlowArgsConfig): Unit = {

    if (params.timestamp != null &&
      (params.listaPod.nonEmpty || params.listaDistributori.nonEmpty ||params.listaUdd.nonEmpty || params.listaDateSW.nonEmpty || params.listaDateNA.nonEmpty || params.listaCoppieDistrUdd.nonEmpty))
      throw new IllegalArgumentException("Se viene specificato un timestamp allora non sono ammessi altri parametri")

    if ((params.listaDateSW.nonEmpty || params.listaDateNA.nonEmpty) && (params.listaPod.nonEmpty && params.listaDistributori.nonEmpty && params.listaUdd.nonEmpty))
      throw new IllegalArgumentException("Se vengono specificate delle date non è possibile fornire contemporaneamente liste di pod, distributori e udd")

    if ((params.listaDateSW.nonEmpty || params.listaDateNA.nonEmpty) && (params.listaDistributori.nonEmpty && params.listaUdd.nonEmpty))
      throw new IllegalArgumentException("Se vengono specificate delle date non è possibile fornire contemporaneamente liste di distributori e udd")

    if ((params.listaDateSW.nonEmpty || params.listaDateNA.nonEmpty) && (params.listaPod.nonEmpty && params.listaDistributori.nonEmpty))
      throw new IllegalArgumentException("Se vengono specificate delle date non è possibile fornire contemporaneamente liste di pod e distributori")

    if ((params.listaDateSW.nonEmpty || params.listaDateNA.nonEmpty) && (params.listaPod.nonEmpty && params.listaUdd.nonEmpty))
      throw new IllegalArgumentException("Se vengono specificate delle date non è possibile fornire contemporaneamente liste di pod e udd")

    if (params.listaCoppieDistrUdd.nonEmpty && (params.listaPod.nonEmpty || params.listaDistributori.nonEmpty || params.listaUdd.nonEmpty))
      throw new IllegalArgumentException("Se vengono specificate delle coppie distributore,udd non è possibile fornire contemporaneamente liste di pod, distributori o udd")

    if (params.flowName.equals(STORICI) && params.listaDateSW.length > 2)
      throw new IllegalArgumentException("I flussi storici possono ricevere al massimo due date come parametro per il filtraggio")

    if (params.flowName.equals(STORICI) && params.listaDateSW.length == 2 &&
      params.listaDateSW(0).getMonth.plus(1).getValue != (params.listaDateSW(1).getMonth.getValue))
      throw new IllegalArgumentException("Le due date passate come parametro per il filtraggio dei flussi storici devono essere di due mesi consecutivi")

    if (params.flowName.equals(STORICI) && params.listaDateSW.length == 2 &&
      (params.listaDateSW(0).getDayOfMonth != 1 || params.listaDateSW(1).getDayOfMonth != 1))
      throw new IllegalArgumentException("Le due date passate come parametro per il filtraggio dei flussi storici devono riferirsi al primo giorno del relativo mese")

    if (params.flowName.equals(STORICI) && params.listaDateNA.nonEmpty)
      throw new IllegalArgumentException("I flussi storici non supportano il parametro -F (--date-funzionali-na)")

    if (!params.runOrdinaria && params.timestamp == null && params.listaDateSW.isEmpty && params.listaDateNA.isEmpty)
      throw new IllegalArgumentException("Lanciare la run ordinaria oppure specificare delle opportune date")
  }

}