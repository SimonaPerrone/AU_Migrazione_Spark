package it.au.misure.ee_switching.flow

import it.au.misure.ee_switching.args.FlowArgsConfig
import it.au.misure.ee_switching.filterPod.FilterPodFactory
import it.au.misure.ee_switching.model.schema.hive.{StoriciCompressedSchema, StoriciSchema}
import it.au.misure.ee_switching.model.schema.xml.StoriciXMLSchema
import it.au.misure.ee_switching.utility.Constants.{FILENAME_TIMESTAMP_PATTERN, ITALIAN_DATE_PATTERN, PLACEHOLDER_PROGRESSIVO, STORICI, XML_CHUNK_NAME_FIELD}
import it.au.misure.ee_switching.utility.{FileUtility, FlowUtility, PropertyUtility, ValidateUtility}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}
import org.apache.spark.sql.types.StringType

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.zone.ZoneOffsetTransition
import it.au.misure.ee_switching.utility.FlowUtility.buildStoriciDatiPodXmlNode
import it.au.misure.ee_switching.utility.environment.Environment

import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.collection.mutable.ArrayBuffer
import scala.xml.XML
import scala.util.{Failure, Success, Try}
import Array._

object StoriciFlow extends Flow {

  override val flowName: String = STORICI
  override val hiveTableName: String = PropertyUtility.getTableStorici
  override val maxNPodsPerXmlFile: Int = PropertyUtility.getMaxNPodsPerXmlFileStorici

  override def run(params: FlowArgsConfig): Unit = {
    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext
    val timestampRun = LocalDateTime.now(ZoneId.of(PropertyUtility.getTimeZone)).format(DateTimeFormatter.ofPattern(FILENAME_TIMESTAMP_PATTERN))
    FileUtility.cleanTmpFolder(flowName)

    var df = loadData
    df = FilterPodFactory.filter(df, flowName, params)//.persist(StorageLevel.MEMORY_AND_DISK)
    emptyCheck(df, params)

    df = getLastDataVersion(df)

    df = assignPodToXmlChunk(df, timestampRun)
    val aggregatedEnergiesDf = aggregatePodsMonthEnergy(df)
    val xmlChunkNodesRdd = createXmlChunkNodes(aggregatedEnergiesDf)
    val xmlChunkFilesRdd = FileUtility.writeXmlFiles(xmlChunkNodesRdd, flowName)
    val validatedXmlChunksRdd = ValidateUtility.validateXmlFiles(xmlChunkFilesRdd, flowName)
    val reportRdd = FileUtility.writeZipFiles(validatedXmlChunksRdd)
    FlowUtility.writeReport(reportRdd)
  }

  override def getLastDataVersion(df: DataFrame) : DataFrame = {
    val leadColName: String = "lead_column"
    val windowSpec = Window.partitionBy(df.col(StoriciSchema.pod14), df.col(StoriciSchema.d_data_decorrenza), df.col(StoriciSchema.data_misura))
      .orderBy(StoriciSchema.d_caricamento)
    df.withColumn(leadColName, lead(StoriciSchema.d_caricamento,1).over(windowSpec))
      .where(col(leadColName).isNull)
  }

  // adding information of the corresponding output xml chunk to each entry
  override def assignPodToXmlChunk(df: DataFrame, timestampRun: String): DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._
    val xmlChunksDf = df.rdd.map(row => (getXmlBaseName(row, timestampRun), (row.getAs[String](StoriciSchema.pod14), row.getAs[String](StoriciSchema.d_data_decorrenza))))
      .groupByKey().flatMap({ case (xmlBaseName, podsDecorrenzeIterable) => {
      val podsDecorrenze: List[(String,String)] = podsDecorrenzeIterable.toList.distinct
      var progressivoChunk: Int = 0
      val chunks: ArrayBuffer[(String, ArrayBuffer[(String,String)])] = ArrayBuffer() // Array[chunkNameXml, Array[(ID Pod, dataDecorrenza)]
      var chunkName: String = ""
      for (podDecorrenzaIndex <- podsDecorrenze.indices) {
        if (podDecorrenzaIndex % maxNPodsPerXmlFile == 0) {
          progressivoChunk += 1
          chunkName = xmlBaseName.replace(PLACEHOLDER_PROGRESSIVO, progressivoChunk.toString)
          chunks.append((chunkName, ArrayBuffer()))
        }
        chunks.last._2.append(podsDecorrenze(podDecorrenzaIndex))
      }
      chunks.flatMap(chunk => chunk._2.map(podDecorrenza => (podDecorrenza._1, podDecorrenza._2, chunk._1))) // RDD[(Id Pod, dataDecorrenza, chunkNameXml)]
    }}).toDF("id_pod_to_join", "d_data_decorrenza_to_join", XML_CHUNK_NAME_FIELD)
    df.join(xmlChunksDf, df.col(StoriciSchema.pod14) === xmlChunksDf.col("id_pod_to_join")
      && df.col(StoriciSchema.d_data_decorrenza) === xmlChunksDf.col("d_data_decorrenza_to_join"))
  }

  override def getXmlBaseName(row: Row, timestampRun: String): String = {
    s"${row.getAs[String](StoriciSchema.piva_distr)}_${row.getAs[String](StoriciSchema.piva_udd)}_${row.getAs[String](StoriciSchema.annomese_sw)}_" +
      s"${row.getAs[String](StoriciSchema.nome_flusso)}_${timestampRun}_${PLACEHOLDER_PROGRESSIVO}${row.getAs[String](StoriciSchema.dp)}.xml"
  }

  // aggregazione misure mensili di un pod in un unico tag xml (Ea), restituisce un Dataframe dove ogni riga contiene un AnnoMese di misure di un determinato pod
  def aggregatePodsMonthEnergy(df: DataFrame): DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext

    sqlContext.createDataFrame(
     df.rdd
        .keyBy(entry => (entry.getAs[String](StoriciSchema.pod14), entry.getAs[String](StoriciSchema.d_data_decorrenza), entry.getAs[String](StoriciSchema.data_misura).takeRight(7))) // aggregazione misure dell'annoMese per ogni pod con una certa data_decorrenza
        .groupByKey()
        .map( { case ((podId, dataDecorrenza, annoMeseMisura), dailyEnergies) => {
          var monthEnergyXmlMap: mutable.Map[Int, String] = mutable.Map()
          for (dailyEnergy <- dailyEnergies) {
            val (day, xmlTag) = getDailyEnergyXmlTag(dailyEnergy)
            if (day != -1)
              monthEnergyXmlMap += (day -> xmlTag)
          }
          val orderedMonthEnergyXmlArray: ListMap[Int, String] = ListMap(monthEnergyXmlMap.toSeq.sortBy(_._1):_*) // ordinamento della mappa mutabile creata appena sopra per ordinare i giorni del mese
          var monthEnergyXmlTag: String = ""
          for ((day, xmlTag) <- orderedMonthEnergyXmlArray)
              monthEnergyXmlTag += xmlTag

          Row.fromSeq(dailyEnergies.head.toSeq ++ Seq(monthEnergyXmlTag))
        }}), df.schema.add(StoriciXMLSchema.Ea, StringType, nullable = true)
    ).selectExpr((List(XML_CHUNK_NAME_FIELD, StoriciXMLSchema.Ea.toString) ::: StoriciCompressedSchema.getValues):_*)
  }

  // aggregazione misure mensili di un pod in un unico tag xml (Ea), restituisce un Dataframe dove ogni riga contiene un AnnoMese di misure di un determinato pod
//  def aggregatePodsMonthEnergy(df: DataFrame) (implicit sc: SparkContext, sqlContext: SQLContext): DataFrame= {
//    sqlContext.createDataFrame(
//      df.rdd
//        .keyBy(entry => (entry.getAs[String](StoriciSchema.pod14), entry.getAs[String](StoriciSchema.d_data_decorrenza))) // aggregazione misure dell'intero anno per ogni pod con una certa data_decorrenza
////        .keyBy(entry => (entry.getAs[String](StoriciSchema.pod14) + "_" + entry.getAs[String](StoriciSchema.d_data_decorrenza))) // aggregazione misure dell'intero anno per ogni pod con una certa data_decorrenza
//        .groupByKey(sc.defaultParallelism * 3)
//        .flatMap( { case ((podId, dataDecorrenza), yearDailyEnergies) => {
////        .flatMap( { case ((podId_dataDecorrenza), yearDailyEnergies) => {
//          yearDailyEnergies.toList
//            .map(row => (row.getAs[String](StoriciSchema.data_misura).takeRight(7), row))
//            .groupBy(_._1).toList // aggregazione delle misure per annoMese
//            .map(_._2.map(_._2))
//            .map(monthDailyEnergies => {
//              var monthEnergyXmlMap: mutable.Map[Int, String] = mutable.Map()
//              for (dailyEnergy <- monthDailyEnergies) {
//                val (day, xmlTag) = getDailyEnergyXmlTag(dailyEnergy)
//                if (day != -1)
//                  monthEnergyXmlMap += (day -> xmlTag)
//              }
//              val orderedMonthEnergyXmlArray: ListMap[Int, String] = ListMap(monthEnergyXmlMap.toSeq.sortBy(_._1):_*) // ordinamento della mappa mutabile creata appena sopra per ordinare i giorni del mese
//              var monthEnergyXmlTag: String = ""
//              for ((day, xmlTag) <- orderedMonthEnergyXmlArray)
//                monthEnergyXmlTag += xmlTag
//
//              Row.fromSeq(monthDailyEnergies.head.toSeq ++ Seq(monthEnergyXmlTag))
//            })
//        }}), df.schema.add(StoriciXMLSchema.Ea, StringType, nullable = true)
//    ).selectExpr((List(XML_CHUNK_NAME_FIELD, StoriciXMLSchema.Ea.toString) ::: StoriciCompressedSchema.getValues):_*)
//  }

  // creazione tag xml contenente le energie di un singolo giorno
  def getDailyEnergyXmlTag(dayEnergyRow: Row): (Int, String) = {
    val parsingDataMisura: Try[LocalDate] = Try(LocalDate.parse(dayEnergyRow.getAs[String](StoriciSchema.data_misura), DateTimeFormatter.ofPattern(ITALIAN_DATE_PATTERN)))
    val dataMisura: LocalDate = parsingDataMisura match {
      case Success(data) => data
      case Failure(exception) => null
    }
    if (dataMisura == null)
      return (-1, "")

    val dayType: Int = getDayType(dataMisura) // ottengo informazioni rispetto ai giorni di transizione ora solare/legale (DST)
    val giornoMisura: String = dataMisura.toString.takeRight(2)
    var xmlTag: String = ""
    val prefix: String = "e"

    if (dayType == 0 || dayType == 1) { // giorno della prima transizione di DST o giorno normale
      xmlTag = if (dayType == 0) s"<${StoriciXMLSchema.Ea}" else s"<${StoriciXMLSchema.Ea} Dst=" + "\"" + "1" + "\""
      val fasceQuartorarie: Array[Int] = if (dayType == 0) range(1, 97) else concat(range(13, 97), range(1, 9))
      for (fascia <- fasceQuartorarie) {
        val energiaFascia = dayEnergyRow.getAs[String](prefix + fascia.toString)
        if (energiaFascia != null && !energiaFascia.toLowerCase.equals("null") && energiaFascia.nonEmpty) {
          xmlTag += s" ${prefix.toUpperCase}${fascia.toString}=" + "\"" + formatEnergiaFascia(energiaFascia) + "\""
        }
      }
      xmlTag += s">${giornoMisura}</${StoriciXMLSchema.Ea}>"

    } else { // dayType == 2 : giorno della seconda transizione di DST
      var xmlTagPart1 = s"<${StoriciXMLSchema.Ea} Dst=" + "\"" + "2" + "\""
      val fasceQuartorariePart1: Array[(Int, Int)] = range(1, 13).map(n => if(n <= 8) (n, n) else (n + 88, n))
      for ((fasciaFrom, fasciaTo) <- fasceQuartorariePart1) {
        val energiaFascia = dayEnergyRow.getAs[String](prefix + fasciaFrom.toString)
        if (energiaFascia != null && !energiaFascia.toLowerCase.equals("null") && energiaFascia.nonEmpty) {
          xmlTagPart1 += s" ${prefix.toUpperCase}${fasciaTo.toString}=" + "\"" + formatEnergiaFascia(energiaFascia) + "\""
        }
      }
      xmlTagPart1 += s">${giornoMisura}</${StoriciXMLSchema.Ea}>"

      var xmlTagPart2 = s"<${StoriciXMLSchema.Ea} Dst=" + "\"" + "3" + "\""
      val fasceQuartorariePart2: Array[Int] = range(9, 97)
      for (fascia <- fasceQuartorariePart2) {
        val energiaFascia = dayEnergyRow.getAs[String](prefix + fascia.toString)
        if (energiaFascia != null && !energiaFascia.toLowerCase.equals("null") && energiaFascia.nonEmpty) {
          xmlTagPart2 += s" ${prefix.toUpperCase}${fascia.toString}=" + "\"" + formatEnergiaFascia(energiaFascia) + "\""
        }
      }
      xmlTagPart2 += s">${giornoMisura}</${StoriciXMLSchema.Ea}>"

      xmlTag = xmlTagPart1 + xmlTagPart2
    }

    (giornoMisura.toInt, xmlTag)
  }

  def getDayType(data: LocalDate): Int = {
    val zoneId: ZoneId = ZoneId.of(PropertyUtility.getTimeZone)
    val rules = zoneId.getRules
    val firstDayOfYear: LocalDate = LocalDate.parse(s"${data.getYear.toString}-01-01") // ottengo primo giorno dell'anno della data passata come parametro di input
    val firstInstantOfYear: Instant = firstDayOfYear.atStartOfDay(zoneId).toInstant
    val firstTransitionDate: ZoneOffsetTransition = rules.nextTransition(firstInstantOfYear)
    val secondTransitionDate: ZoneOffsetTransition = rules.nextTransition(firstTransitionDate.getInstant)

    if (data.equals(firstTransitionDate.getInstant.atZone(zoneId).toLocalDate)) 1
    else if (data.equals(secondTransitionDate.getInstant.atZone(zoneId).toLocalDate)) 2
    else 0
  }

  def formatEnergiaFascia(s: String): String = "%.3f".format(s.toDouble).replace(".",",")

  // from Dataframe to RDD[(chunkNameXml, List[ID Pods in chunk Xml], DatiPodsXmlTag)]
  override def createXmlChunkNodes(df: DataFrame): RDD[(String, List[String], String)] = {
    df.rdd
      .keyBy(row => row.getAs[String](XML_CHUNK_NAME_FIELD))
      .groupByKey()
      .map( { case (chunkName, rows) => (chunkName, rows.map(row => buildStoriciDatiPodXmlNode(row))) })
      .map( { case (chunkName, datiPodTags) => {
        val orderedMonthlyMeasuresPods: List[(String, String)] =
          datiPodTags.groupBy(_._1).toList
            .map( { case (pod, measures) => {
              (pod,
                measures.toList.map(_._2) // considero solo le misure
                  .map(montlhyMeasureXmlTag => ((XML.loadString(montlhyMeasureXmlTag) \\ StoriciXMLSchema.MeseAnno).text.takeRight(4) + (XML.loadString(montlhyMeasureXmlTag) \\ StoriciXMLSchema.MeseAnno).text.take(2), montlhyMeasureXmlTag)) // costruisco annoMese per ordinamento
                  .sortBy(_._1) // ordino per annoMese
                  .map(_._2) // tengo solo tagXml ordinati
                  .mkString("") // concatenazione dei tagXml ordinati
              )
            }})
        (chunkName, orderedMonthlyMeasuresPods.map(_._1), orderedMonthlyMeasuresPods.map(_._2).mkString(""))
      }})
  }
}
