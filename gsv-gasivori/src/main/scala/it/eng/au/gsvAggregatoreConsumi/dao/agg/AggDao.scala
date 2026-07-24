package it.eng.au.gsvAggregatoreConsumi.dao.agg

import it.eng.au.gsvAggregatoreConsumi.dao.Dao
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait AggDao extends Dao {

  val EXECUTION_ID = "executionid"

  def readParquet(): DataFrame = {

    val executionId = getLastExecutionId

    Environment.getSpark.sqlContext.read.parquet(tablePath)
      .where(col(EXECUTION_ID) === executionId)
      .selectExpr(columns: _*)
  }

  def getLastExecutionId: Long = {
    val partition = Environment.getSpark.sql(s"SHOW PARTITIONS $tableName")
      .filter(col("partition").contains("session=CCG"))
      .rdd
      .map { row =>
      val partitionString = row.getString(0)
      val partitionParts = partitionString.split("executionid=")
      partitionParts(1)
      }
      .max
      .toLong

    partition

  }
}
