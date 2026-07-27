package it.eng.au.ccgPubblicazione.dao

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait PartitionedDao extends Dao {
  val partitionField: String

  def readPartition(executionid: String): DataFrame = {
    readTable.filter(col(partitionField) === executionid)
  }
}
