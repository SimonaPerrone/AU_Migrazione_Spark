package it.eng.au.cceCalcolo.dao.calcoloOutput

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.calcoloOutput.CceCalcTrackSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class CceCalcTrackDAO extends Dao {
  override val tablePath: String = Properties.getCceCalcTrackTablePath
  override val tableName: String = Properties.getCceCalcTrackTableName
  override val columns: List[String] = CceCalcTrackSchema.getValues
  val executionId = "executionId"

  def getCceCalcTrack: DataFrame = {
    val df = Environment.spark.read.parquet(tablePath)

    df
  }

  def writeCalcTrack(df: DataFrame): Unit = {
    df
      .withColumn(executionId, lit(Environment.executionId.toString))
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .save(tablePath)

    Environment.spark.sql(s"REFRESH $tableName")
  }
}
