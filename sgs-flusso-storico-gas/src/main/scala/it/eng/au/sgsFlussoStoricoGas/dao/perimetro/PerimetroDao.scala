package it.eng.au.sgsFlussoStoricoGas.dao.perimetro

import it.eng.au.sgsFlussoStoricoGas.dao.Dao
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import scala.util.Try

trait PerimetroDao extends Dao {

  val EXECUTION_ID = "executionid"
  val tableName: String

  def readLastExecutionIdLastAnnomeseDF(lastAnnoMese: String): DataFrame = {

    val partitionsDF = Environment.getSpark.sql(s"SHOW PARTITIONS $tableName").filter(col("partition").contains(lastAnnoMese))

    val lastExecId = partitionsDF
      .rdd
      .flatMap { row =>
        Option(row.getString(0)) // Gestisce il caso in cui la colonna è null
          .flatMap { partitionString =>
            val partitionParts = partitionString.split("executionid=")
            if (partitionParts.length > 1) {
              Try(partitionParts(1).toLong).toOption
            } else {
              None
            }
          }
      }

    val lastExecIdValue = if (lastExecId.isEmpty()) {
      // Valore di default in caso di RDD vuoto
      0L
    } else {
      lastExecId.max
    }

    Environment.getSpark.sqlContext.read.parquet(tablePath).filter(col(EXECUTION_ID)===lastExecIdValue)

  }

  def readLastVersionDF(lastExecId: Long): DataFrame = {
    Environment.getSpark.read.parquet(tablePath).filter(col(EXECUTION_ID)===lastExecId)
  }

}
