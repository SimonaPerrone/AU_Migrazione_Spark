package it.sferanet.au.filterPdr


import it.sferanet.au.filterPdr.input.schema.ExclusioneFilePdrPdrSchema
import it.sferanet.au.filterPdr.input.struct.{ExclusioneFilePdrFileStruct, ExclusioneFilePdrPdrStruct}
import it.sferanet.au.model.Flow
import it.sferanet.au.schema.PdrMassivoSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

import scala.language.postfixOps

@deprecated("Use it.sferanet.au.filterPdr.MeasureFilter instead.")
class Filter7ExclusioneFilePdr extends FilterPdr {
  private val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  lazy val filePathPdrToExclude: String = Environment.getIgnorePdrMeasurePdrCsvPath
  lazy val filePathMeasureLocalFileToExclude: String = Environment.getIgnorePdrMeasureFileCsvPath
  lazy val flagIgnorePdrMeasure: String = Environment.isIgnorePdrMeasuresFilterEnabled


  lazy val pdrToExcludeDf: DataFrame = Environment.getSqlContext.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .format("csv").schema(ExclusioneFilePdrPdrStruct.struct).load(filePathPdrToExclude)
    .cache()
  lazy val localFileToExcludeDf: DataFrame = Environment.getSqlContext.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
    .format("csv").schema(ExclusioneFilePdrFileStruct.struct).load(filePathMeasureLocalFileToExclude)
    .cache()

  override def filter(measures: RDD[Flow]): RDD[Flow] = {

    if (getBooleanByString(flagIgnorePdrMeasure).get) {
      val measuresFilteredByPdr: RDD[Flow] = removeMeasuresByPdr(measures, pdrToExcludeDf.distinct.rdd)
      val measureFilterByLocalFile: RDD[Flow] = removeMeasuresByLocalFile(measuresFilteredByPdr, localFileToExcludeDf.distinct.rdd)
      measureFilterByLocalFile

    } else measures

  }

  override def filterPdrMassivo(pdrMassivo: DataFrame): DataFrame = {
    if (getBooleanByString(flagIgnorePdrMeasure).get) {
      pdrMassivo.join(pdrToExcludeDf, pdrToExcludeDf(ExclusioneFilePdrPdrSchema.pdr.toString) === pdrMassivo(PdrMassivoSchema.codice_pdr), "left")
        .filter(col(ExclusioneFilePdrPdrSchema.pdr.toString) isNull)
        .drop(col(ExclusioneFilePdrPdrSchema.pdr.toString))
    } else pdrMassivo
  }

  override protected def getPdrs: RDD[String] = ???

  /**
   *
   * @param measures     rdd con tutte le misure
   * @param pdrToExclude codici pdr
   * @return misure senza quelle appartenenti ai pdr presenti in  @link pdrToExclude
   */
  private def removeMeasuresByPdr(measures: RDD[Flow], pdrToExclude: RDD[Row]): RDD[Flow] = {
    val measuresByPdr: RDD[(String, Flow)] = measures.keyBy(_.pdr)
    val pdrRDD: RDD[(String, Unit)] = pdrToExclude.map(x => (x.getString(0), ()))

    measuresByPdr

      /** dalle misure rimuovo tutte quelle i cui pdr sono nella lista pdr in input */
      .leftOuterJoin(pdrRDD).filter(_._2._2.isEmpty).map(_._2._1)

  }

  /**
   *
   * @param measures           rdd con tutte le misure
   * @param localFileToExclude local_path delle misurazioni
   * @return misure senza quelle i cui local path sono presenti in  @link localFileToExclude
   */
  private def removeMeasuresByLocalFile(measures: RDD[Flow], localFileToExclude: RDD[Row]): RDD[Flow] = {
    val measuresByLocalFile: RDD[(Option[String], Flow)] = measures.keyBy(_.local_file)
    val localFileRDD: RDD[(Option[String], Unit)] = localFileToExclude.map(x => (Option(x.getString(0)), ()))

    measuresByLocalFile

      /** dalle misure rimuovo tutte quelle i cui LOCAL FILE sono nella lista dei file in input */
      .leftOuterJoin(localFileRDD).filter(_._2._2.isEmpty).map(_._2._1)

  }

  /**
   *
   * @param flag
   * @return @flag in boolean se riesce a convertirlo
   *         altrimenti ritorna false e lancia un WARN con il seguente errore: "value of ignorePdrMeasure.enable property is not in (true, false) => i will not filtering (Req 2)"
   */
  private def getBooleanByString(flag: String): Option[Boolean] = {
    try {
      Some(flag.toBoolean)
    } catch {
      case _: Throwable => {
        log.error("value of ignorePdrMeasure.enable property is not in (true, false) => i will not filtering (Req 2)");
        System.exit(1)
        None
      }
    }
  }
}
