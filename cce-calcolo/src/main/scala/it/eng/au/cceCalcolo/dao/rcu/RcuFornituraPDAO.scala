package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuFornituraPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.parsedate.DateUtility.fillNullDates
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame

class RcuFornituraPDAO extends Dao {
  override val tablePath: String = Properties.getRcuFornituraPTablePath
  override val tableName: String = Properties.getRcuFornituraPTableName
  override val columns: List[String] = RcuFornituraPSchema.getValues

  def getRcuFornituraP: DataFrame = {
    val df = fillNullDates(Environment.spark.read.parquet(tablePath), RcuFornituraPSchema.d_inizio_titolarita, RcuFornituraPSchema.d_fine_titolarita)
      .selectExpr(columns:_*)

    df
  }
}
