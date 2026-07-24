package it.eng.au.pubblicazionePcg.utility

import it.eng.au.pubblicazionePcg.args.CLIArgsConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

trait SparkImplicit {
  @transient var _sc: SparkContext = _
  @transient var _sqlContext: HiveContext = _
  @transient var _fs: FileSystem = _
  implicit lazy val sc: SparkContext = _sc
  implicit lazy val sqlContext: SQLContext = _sqlContext
  implicit lazy val fs: FileSystem = _fs
  implicit val jobProperties: Properties = new Properties()

  def initialize(appName: String, cliArgs: CLIArgsConfig): Unit = {

    val conf = new SparkConf().setAppName(appName)
    conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    _sc = SparkContext.getOrCreate(conf)
    _sqlContext = new HiveContext(_sc)

    _fs = FileSystem.get(_sc.hadoopConfiguration)

    jobProperties.setProperty("offset.mese", cliArgs.offestMese.get)
    jobProperties.setProperty("num.linesPerCsv", cliArgs.numLinesPerCsv.get)
    jobProperties.setProperty("sbg.type", cliArgs.sbgType.get)
    jobProperties.setProperty("sbgmisure.hdfs.path", cliArgs.sbgMisureHdfsPath.get)
    jobProperties.setProperty("isilon.basepath.out", cliArgs.isilonBasepathOut.get)
    jobProperties.setProperty("hdfs.output.basepath.infoLog", cliArgs.hdfsOutputBasepathInfoLog.get)
    jobProperties.setProperty("dailyConsumptionSbg.tableName", cliArgs.dailyConsumptionTableName.get)
  }

}
