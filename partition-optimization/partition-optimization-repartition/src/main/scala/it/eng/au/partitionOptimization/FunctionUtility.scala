package it.eng.au.partitionOptimization

import org.apache.hadoop.fs.Path
import org.apache.log4j.{FileAppender, Logger, SimpleLayout}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

object FunctionUtility {

  def createLogForTable(table: String, process: String): FileAppender = {
    val nameTable = table.split("\\.").mkString("-")
    val layout = new SimpleLayout()
    val appender = new FileAppender(layout, s"${Environment.getPathToLog}/${nameTable}_${process}_${Environment.getPartitionDate.toString("yyyy-MM-dd_HH:mm:ss")}.log", true)
    appender
  }

  /**
   * @param table
   * @return list of partition
   *
   *         Es:
   *         show partitions output: List([anno_competenza=2018/executionid=00000000000001], [anno_competenza=2021/executionid=1616663722376])
   */
  def getListPartitions(table: String): List[String] = {
    Environment.getSpark
      .sql(s"SHOW PARTITIONS $table")
      .collect
      .toList
      .map(_.getString(0))

  }

  /**
   *
   * @param listPartitions
   * @return list columns
   *         es input: List([anno_competenza=2018/executionid=00000000000001], [anno_competenza=2021/executionid=1616663722376])
   *         output: List([anno_competenza, executionid])
   */
  def getPartitionColumns(listPartitions: List[String]): List[String] = {
    listPartitions
      .head
      .split("/")
      .map(_.split("=").head)
      .toList
  }

  def getPathInputFile(table: String): String = {
    Environment.getSpark.sql(s"desc formatted $table").toDF
      .filter(col("col_name") === "Location")
      .collect()(0)(1)
      .toString
  }

  def writeDf(df: DataFrame, numOutputPartition: Int, partition: String, schema: StructType, pathOutputFile: String, partitionColumn: List[String], repartition: Boolean = true): Unit = {
    var dfAddPartition = df

    partition.split("/").foreach(fieldValue => {
      val splitFieldValue = fieldValue.split("=")
      val field = splitFieldValue.head
      val value = splitFieldValue.last
      val dataType = schema(field).dataType

      dfAddPartition = dfAddPartition
        .withColumn(field, lit(value).cast(dataType))
    })

    val dfRepartition =
      if (repartition)
        dfAddPartition.repartition(numOutputPartition)
      else
        dfAddPartition

    dfRepartition
      .write
      .partitionBy(partitionColumn: _*)
      .mode("overwrite")
      .parquet(s"$pathOutputFile")
  }

  def moveTable(inputPath: String, outputPath: String): Boolean = {
    val input = new Path(inputPath)
    val output = new Path(outputPath)

    val fs = Environment.getFs

    if (fs.exists(output))
      fs.delete(output, true)

    fs.rename(input, output)

  }
}
