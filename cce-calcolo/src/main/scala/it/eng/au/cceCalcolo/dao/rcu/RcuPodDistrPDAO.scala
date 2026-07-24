package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuPodDistrPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame

class RcuPodDistrPDAO extends Dao {
  override val tablePath: String = Properties.getRcuPodDistrPTablePath
  override val tableName: String = Properties.getRcuPodDistrPTableName
  override val columns: List[String] = RcuPodDistrPSchema.getValues

  def getRcuPodDistrP: DataFrame = {
    val df = Environment.spark.read.parquet(tablePath)
      .selectExpr(columns:_*)

    df
  }
}
