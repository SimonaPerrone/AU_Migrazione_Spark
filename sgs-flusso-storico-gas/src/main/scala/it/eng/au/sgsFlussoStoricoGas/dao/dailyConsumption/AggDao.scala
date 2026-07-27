package it.eng.au.sgsFlussoStoricoGas.dao.dailyConsumption

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait AggDao extends Dao {

  val EXECUTION_ID = "executionid"
  val tableName = ""

  override def readTable: DataFrame = {

    val executionId = getLastExecutionId

    Environment.getSpark.sqlContext.read.parquet(tablePath)
      .where(col(EXECUTION_ID) === executionId)
  }

  private def getLastExecutionId: Long = {
    val partition = Environment.getSpark.sql(s"SHOW PARTITIONS $tableName")
      .filter(col("partition").like("%session=CCG%"))
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
