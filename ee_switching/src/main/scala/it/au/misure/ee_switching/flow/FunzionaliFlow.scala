package it.au.misure.ee_switching.flow

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import it.au.misure.ee_switching.args.FlowArgsConfig
import it.au.misure.ee_switching.utility.{FileUtility, FlowUtility, PropertyUtility, ValidateUtility}
import it.au.misure.ee_switching.filterPod.FilterPodFactory
import it.au.misure.ee_switching.model.schema.hive.{FunzionaliCompressedSchema, FunzionaliSchema}
import it.au.misure.ee_switching.utility.Constants.{FILENAME_TIMESTAMP_PATTERN, FUNZIONALI, PLACEHOLDER_PROGRESSIVO, XML_CHUNK_NAME_FIELD}
import it.au.misure.ee_switching.utility.FlowUtility.buildFunzionaliDatiPodXmlNode
import it.au.misure.ee_switching.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

object FunzionaliFlow extends Flow {

  override val flowName: String = FUNZIONALI
  override val hiveTableName: String = PropertyUtility.getTableFunzionali
  override val maxNPodsPerXmlFile: Int = PropertyUtility.getMaxNPodsPerXmlFileFunzionali

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
    val xmlChunkNodesRdd = createXmlChunkNodes(df)
    val xmlChunkFilesRdd = FileUtility.writeXmlFiles(xmlChunkNodesRdd, flowName)
    val validatedXmlChunksRdd = ValidateUtility.validateXmlFiles(xmlChunkFilesRdd, flowName)
    val reportRdd = FileUtility.writeZipFiles(validatedXmlChunksRdd)
    FlowUtility.writeReport(reportRdd)
  }

  override def getLastDataVersion(df: DataFrame) : DataFrame = {
    val leadColName: String = "lead_column"
    val windowSpec = Window.partitionBy(df.col(FunzionaliSchema.pod14), df.col(FunzionaliSchema.d_data_decorrenza))
      .orderBy(FunzionaliSchema.d_caricamento)
    df.withColumn(leadColName, lead(FunzionaliSchema.d_caricamento,1).over(windowSpec))
      .where(col(leadColName).isNull)
  }

  // adding information of the corresponding output xml chunk to each entry
  override def assignPodToXmlChunk(df: DataFrame, timestampRun: String): DataFrame = {
    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._
    val xmlChunksDf = df.rdd.map(row => (getXmlBaseName(row, timestampRun), (row.getAs[String](FunzionaliSchema.pod14), row.getAs[String](FunzionaliSchema.d_data_decorrenza))))
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
    df.join(xmlChunksDf, df.col(FunzionaliSchema.pod14) === xmlChunksDf.col("id_pod_to_join")
      && df.col(FunzionaliSchema.d_data_decorrenza) === xmlChunksDf.col("d_data_decorrenza_to_join"))
      .select(XML_CHUNK_NAME_FIELD, FunzionaliCompressedSchema.getValues:_*)
  }

  override def getXmlBaseName(row: Row, timestampRun: String): String = {
    s"${row.getAs[String](FunzionaliSchema.piva_distr)}_${row.getAs[String](FunzionaliSchema.piva_udd)}_${row.getAs[String](FunzionaliSchema.annomese_sw)}_" +
      s"${row.getAs[String](FunzionaliSchema.nome_flusso)}_${timestampRun}_${PLACEHOLDER_PROGRESSIVO}${row.getAs[String](FunzionaliSchema.t_cod_contr_disp)}.xml"
  }

  // from Dataframe to RDD[(chunkNameXml, List[ID Pods in chunk Xml], DatiPodsXmlTag)]
  override def createXmlChunkNodes(df: DataFrame): RDD[(String, List[String], String)] = {
    df.rdd
      .keyBy(row => row.getAs[String](XML_CHUNK_NAME_FIELD))
      .groupByKey()
      .map( { case (chunkName, rows) => (chunkName, rows.map(row => buildFunzionaliDatiPodXmlNode(row))) })
      .map( { case (chunkName, datiPodTags) => (chunkName, datiPodTags.map(_._1).toList, datiPodTags.map(_._2).mkString("")) } )
  }

}
