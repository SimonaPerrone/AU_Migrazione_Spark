package it.au.misure.eng.ammissibilita

import it.au.misure.cli.{CommandLineOptions, CommonsCliUtils}
import it.au.misure.commons.cli.CommandLine
import it.au.misure.eng.args.AmmissibilitaParameters
import it.au.misure.eng.model._
import it.au.misure.eng.utility.{Constants, PropertyUtility, SparkImplicit}
import it.au.misure.util.LoggingSupport
import org.apache.spark.rdd.RDD

import java.io.File
import scala.util.matching.Regex

trait CheckAmmissibilitaEE extends SparkImplicit with LoggingSupport {

  val filenameRegex: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d+)_([A-Za-z0-9]+)_(\\d+)_(\\d+)((DP|dp|Dp|dP)\\d+)_([A-Za-z0-9]+).(xml|XML)$".r
  val filenameRegexSMIS: Regex = "^([A-Za-z0-9]+)_([A-Za-z0-9]+)_(\\d+)_(SMIS)_(\\d+)_(\\d+)((DP|dp|Dp|dP)\\d+).(xml|XML)$".r

  def parseAmmissibilitaArgs(args: Array[String]): AmmissibilitaParameters = {
    val commandLineOptions = new CommandLineOptions()
    val commonsCliUtils = new CommonsCliUtils()
    val commandLine: CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
    val argsObjMaster = new CommonsCliUtils().getArgs(commandLine)

    if (!commandLine.hasOption(commandLineOptions.anno.getOpt) || !commandLine.hasOption(commandLineOptions.mese.getOpt) || !commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
      log.error("**** Bisogna specificare l'anno il mese e il giorno ")
      throw new IllegalArgumentException("**** Bisogna specificare l'anno il mese e il giorno ")
    }

    if (!commandLine.hasOption(commandLineOptions.injection1G.getOpt) && !commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
      log.error("**** Bisogna specificare se 1G/2G")
      throw new IllegalArgumentException("**** Bisogna specificare se 1G/2G")
    }


    AmmissibilitaParameters(
      g = if (commandLine.hasOption(commandLineOptions.injection1G.getOpt)) Constants._1G else Constants._2G,
      year = argsObjMaster.anno,
      month = argsObjMaster.mese,
      day = argsObjMaster.giorno,
      isSmis = commandLine.hasOption(commandLineOptions.smis.getOpt) //if in args there is SS option this is a smis flow then set true
    )
  }

  def getInputOutput(params: AmmissibilitaParameters): (String, String) = {
    val (inputPath, outputPath) = params.g match {
      case Constants._1G => (PropertyUtility.getTmp1GAmmissibilita, PropertyUtility.getOutput1GAmmissibilita)
      case Constants._2G => (PropertyUtility.getTmp2GAmmissibilita, PropertyUtility.getOutput2GAmmissibilita)
    }
    (inputPath, outputPath)
  }

  def getXMLMetadata(file:File, matches: Option[Regex.Match], params: AmmissibilitaParameters): XMLMetadata = {
    val pippo = 1
    new XMLMetadata(
      file = file,
      pivaDistributore = matches.get.group(1),
      pivaUDD = matches.get.group(2),
      annoMese = matches.get.group(3),
      flusso = matches.get.group(4),
      timestamp = matches.get.group(5),
      progressivo = matches.get.group(6),
      codDp = matches.get.group(7),
      sm = matches.get.group(9),
      params = params
    )
  }

  def getXMLMetadataSMIS(file:File, matches: Option[Regex.Match], params: AmmissibilitaParameters): XMLMetadata = new XMLMetadata(
    file = file,
    pivaDistributore = matches.get.group(1),
    pivaUDD = matches.get.group(2),
    annoMese = matches.get.group(3),
    flusso = matches.get.group(4),
    timestamp = matches.get.group(5),
    progressivo = matches.get.group(6),
    codDp = matches.get.group(7),
    sm = null,
    params = params
  )

  def getRDDDaysFolder(inputPath: String, params: AmmissibilitaParameters): RDD[File] = {
    val xmlFolder = new File(inputPath)
    //ex: /mnt/isilonshare_gas/TMG_00010200186/DISTRIBUTORE/TMG_00010200186_00273990168/2019/0101
    // /mnt/isilonshare_gas/TMG_00010200186
    val distributori = xmlFolder.listFiles.toList.filter(_.isDirectory)

    // /mnt/isilonshare_gas/TMG_00010200186/DISTRIBUTORE/TMG_00010200186_00273990168
    val sottesi = distributori.flatMap(dist => dist.listFiles().filter(_.isDirectory).flatMap(_.listFiles().filter(_.isDirectory)))

    // /mnt/isilonshare_gas/TMG_00010200186/DISTRIBUTORE/TMG_00010200186_00273990168/2019
    val years = sc.parallelize(sottesi, (sottesi.length ) )
      .flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == params.year))

    // /mnt/isilonshare_gas/TMG_00010200186/DISTRIBUTORE/TMG_00010200186_00273990168/2019/0101
    val days = years.flatMap(_.listFiles().filter(f => f.isDirectory && f.getName == params.month + params.day))
    days
  }

  def getAmmissibilitaOutputFolder(fileXml: File, inputPath: String, outputPath: String): String = {
    val outFolder = fileXml.getPath.replaceAll("\\\\", "/").replace(inputPath, outputPath)
    outFolder
  }

  def getMessageWithNewLogic(message:ReportMessage):ReportMessage = {
      val bloccante = if (message.bloccante.equals(Constants.BLOCCANTE)) Constants.YES else Constants.NO
      val ammissibile = if (message.bloccante.equals(Constants.BLOCCANTE) || message.bloccante.equals(Constants.NON_BLOCCANTE)) Constants.NO else Constants.YES
      val idx1G = message.cartellaCloud.indexOf("TME_")
      val idx2G = message.cartellaCloud.indexOf("TM2G_")
      val idx = if (idx1G < 0 && idx2G < 0) 0 else Math.max(idx1G, idx2G)
      val cloudPath = message.cartellaCloud.substring(idx)

      message match {
        case podMessage: ReportEsitoPODMessage => podMessage.copy(cartellaCloud = cloudPath, bloccante = bloccante, ammissibilita = ammissibile)
        case fileMessage: ReportEsitoFILEMessage => fileMessage.copy(cartellaCloud = cloudPath, bloccante = bloccante, ammissibilita = ammissibile)
      }
  }
}
