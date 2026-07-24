package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuAziendaPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame

class RcuAziendaPDAO extends Dao {
  override val tablePath: String = Properties.getRcuAziendaPTablePath
  override val tableName: String = Properties.getRcuAziendaPTableName
  override val columns: List[String] = RcuAziendaPSchema.getValues

  def getRcuAziendaP: DataFrame = {
    val df = Environment.spark.read.parquet(tablePath)
      .selectExpr(columns:_*)

    df
  }
}
