package it.eng.au.cceCalcolo.dao.annullamenti

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, SaveMode}

trait CceMoAnnullateDAO extends Dao {

  def writeOnHive(df: DataFrame): Unit = {
    df
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(tablePath)

    Environment.spark.sql(s"REFRESH $tableName")
  }

  def getMoAnnullate: DataFrame = {
    Environment.spark.read.parquet(tablePath)
  }

}
