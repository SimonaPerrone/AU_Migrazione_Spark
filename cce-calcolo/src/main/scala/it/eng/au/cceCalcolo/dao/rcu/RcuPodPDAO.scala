package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuPodPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class RcuPodPDAO extends Dao {
  override val tablePath: String = Properties.getRcuPodPTablePath
  override val tableName: String = Properties.getRcuPodPTableName
  override val columns: List[String] = RcuPodPSchema.getValues

  def getRcuPodP: DataFrame = {
    val df = Environment.spark.read.parquet(tablePath)
      .filter(col(RcuPodPSchema.t_area_rif).isNotNull)
      .selectExpr(columns:_*)

    df
  }
}
