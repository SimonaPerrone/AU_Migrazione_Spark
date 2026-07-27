package it.eng.au.ccgPubblicazione.dao

import it.eng.au.ccgPubblicazione.utility.Constants.CCG
import it.eng.au.ccgPubblicazione.utility.Environment
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

trait SessionDao extends PartitionedDao {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def specificDfFilterPartition: Column
  def specificFilterPartition(listPartitions: List[String]): List[String]

  def readLastPartition: (DataFrame, String) = {
    val listPartitions = Environment.spark
      .sql(s"SHOW PARTITIONS $tableName")
      .collect
      .toList
      .map(_.getString(0))

    //this need to extract last partition value
    // es: input List([session=CCG/anno_competenza=2018/executionid=00000000000001, session=CCG/anno_competenza=2018/executionid=00000000000002]
    //     output 00000000000002
    val valuePartition = specificFilterPartition(listPartitions)
      .map(
        _.split("/")
          .filter(_.contains(partitionField))
          .head
          .split("=").last
      )
      .sorted
      .reverse
      .head

    logger.warn(s"Partition to read $valuePartition")
    (readTable.filter(col(partitionField) === valuePartition && specificDfFilterPartition)
      , valuePartition)

  }

}
