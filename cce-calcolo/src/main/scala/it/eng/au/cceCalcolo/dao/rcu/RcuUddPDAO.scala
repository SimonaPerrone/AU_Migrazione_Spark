package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuUddPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame

class RcuUddPDAO extends Dao {
  override val tablePath: String = Properties.getRcuUddPTablePath
  override val tableName: String = Properties.getRcuUddPTableName
  override val columns: List[String] = RcuUddPSchema.getValues

  def getRcuUddP: DataFrame = {
    val df = Environment.spark.read.parquet(tablePath)
      .selectExpr(columns:_*)

    df
  }
}
