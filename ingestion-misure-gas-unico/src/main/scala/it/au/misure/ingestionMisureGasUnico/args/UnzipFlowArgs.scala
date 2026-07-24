package it.au.misure.ingestionMisureGasUnico.args

import org.apache.log4j.Logger
import scopt.OptionParser

import java.time.LocalDate
import java.time.format._

object UnzipFlowArgs {
  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  def parse(args: Array[String]): UnzipArgsConfig = {
    val parser = new OptionParser[UnzipArgsConfig]("ingestion-misure-gas-unico Unzip") {
      head("ingestion-misure-gas-unico Unzip")

      opt[String]('d', "fromDate") action { (x, c) =>
        c.copy(fromDate = Some(LocalDate.parse(x, DateTimeFormatter.ISO_LOCAL_DATE)))
      } text "fromDate"

      opt[String]('D', "toDate") action { (x, c) =>
        c.copy(toDate = Some(LocalDate.parse(x, DateTimeFormatter.ISO_LOCAL_DATE)))
      } text "toDate"

      opt[String]('f', "flow") action { (x, c) =>
        c.copy(flows = Some(x.split(",").toSet))
      } text "flow"

      opt[Unit]('R', "recovery") action { (x, c) =>
        c.copy(recovery = true)
      } text "recovery"

      opt[Unit]('F', "fileRecovery") action { (x, c) =>
        c.copy(fileRecovery = true)
      } text "Enable procedure to recover a list of files. Use recovery.csv.path setting to select the files to recover."

      opt[Unit]('o', "old") action { (x, c) =>
        c.copy(oldOnly = true)
      } text "old flows only unzip"

      opt[Unit]('s', "standard") action { (x, c) =>
        c.copy(standardAndIgmgAndIgmr = true)
      } text "standard flows and igmg and igmr unzip"

      opt[Unit]('S', "standard-only") action { (x, c) =>
        c.copy(standardOnly = true)
      } text "standard flows only unzip"

      opt[Unit]('I', "igmg-only") action { (x, c) =>
        c.copy(igmgOnly = true)
      } text "igmg flows only unzip"

      opt[Unit]('i', "standard-igmg") action { (x, c) =>
        c.copy(standardAndIgmg = true)
      } text "standard and igmg flows unzip"

      opt[Unit]('G', "igmg-igmr") action { (x, c) =>
        c.copy(igmgAndIgmr = true)
      } text "igmg and igmr flows unzip"

      help('h', "help") text "prints all options"

      checkConfig(c =>
      if (c.fromDate.isEmpty && !c.fileRecovery) failure("Please give fromDate param \"-d yyyy-MM-dd\" as input, or activate file recovery procedure by using -F.")
      else success)
    }
    parser.parse(args, UnzipArgsConfig()) match {
      case Some(c) => c
      case None =>
        logger.error(s"error arguments [${args.mkString(" ")}]")
        throw new IllegalArgumentException(s"error arguments [${args.mkString(" ")}]")
    }
  }
}