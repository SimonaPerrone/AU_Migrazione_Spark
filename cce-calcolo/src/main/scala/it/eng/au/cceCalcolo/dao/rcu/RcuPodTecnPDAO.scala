package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuPodTecnPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit}

class RcuPodTecnPDAO extends Dao {
  override val tablePath: String = Properties.getRcuPodTecnPTablePath
  override val tableName: String = Properties.getRcuPodTecnPTableName
  override val columns: List[String] = RcuPodTecnPSchema.getValues

  def getRcuPodTecnP: DataFrame = {
    val df = Environment.spark.read.parquet(tablePath)
      .filter(coalesce(col(RcuPodTecnPSchema.n_tensione), lit(-1)) >= 0)
      .selectExpr(columns:_*)

    df
  }
}
