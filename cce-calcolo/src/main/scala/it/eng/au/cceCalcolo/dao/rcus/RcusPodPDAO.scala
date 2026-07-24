package it.eng.au.cceCalcolo.dao.rcus

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcus.RcusPodPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class RcusPodPDAO extends Dao {
  override val tablePath: String = Properties.getRcusPodPTablePath
  override val tableName: String = Properties.getRcusPodPTableName
  override val columns: List[String] = RcusPodPSchema.getValues

  def getRcusPodP: DataFrame = {
    val df = Environment.spark.read.parquet(tablePath)
      .filter(col(RcusPodPSchema.t_area_rif).isNotNull)
      .selectExpr(columns:_*)

    df
  }
}
