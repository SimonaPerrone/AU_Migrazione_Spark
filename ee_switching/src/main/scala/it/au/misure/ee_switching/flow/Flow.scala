package it.au.misure.ee_switching.flow

import it.au.misure.ee_switching.utility.PropertyUtility
import it.au.misure.ee_switching.args.FlowArgsConfig
import it.au.misure.ee_switching.utility.environment.Environment
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}


trait Flow {

  @transient val logger: Logger = Logger.getLogger(this.getClass.getName)

  val flowName: String
  val hiveDatabaseName: String = PropertyUtility.getHiveDb
  val hiveTableName: String
  val maxNPodsPerXmlFile: Int

  def run(params: FlowArgsConfig): Unit

  def loadData:DataFrame =  {
    val sc = Environment.getSpark.sparkContext
    val sqlContext = Environment.getSpark.sqlContext
    sqlContext.table(s"$hiveDatabaseName.$hiveTableName")
  }

  def getLastDataVersion(df: DataFrame) : DataFrame

  def emptyCheck(df: DataFrame, params: FlowArgsConfig): Unit = {
    if (df.rdd.isEmpty)
      throw new IllegalArgumentException(s"Non esiste nessuna entries da considerare sulla base dei parametri forniti in input (${params.toString})")
  }

  // adding information of the corresponding output xml chunk to each entry
  def assignPodToXmlChunk(df: DataFrame, timestampRun: String): DataFrame

  def getXmlBaseName(row: Row, timestampRun: String): String

  // from Dataframe to RDD[(chunkNameXml, List[ID Pods in chunk Xml], DatiPodsXmlTag)]
  def createXmlChunkNodes(df: DataFrame): RDD[(String, List[String], String)]

}
