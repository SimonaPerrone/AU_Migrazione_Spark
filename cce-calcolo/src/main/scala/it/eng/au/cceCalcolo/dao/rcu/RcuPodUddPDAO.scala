package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuPodUddPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.environment.Environment.startDateTime
import it.eng.au.cceCalcolo.utility.parsedate.DateUtility.fillNullDates
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date}

class RcuPodUddPDAO extends Dao {
  override val tablePath: String = Properties.getRcuPodUddPTablePath
  override val tableName: String = Properties.getRcuPodUddPTableName
  override val columns: List[String] = RcuPodUddPSchema.getValues

  def getRcuPodUddP: DataFrame = {
    val df = fillNullDates(Environment.spark.read.parquet(tablePath), RcuPodUddPSchema.d_inizio, RcuPodUddPSchema.d_fine)
      .filter(col(RcuPodUddPSchema.d_fine) >= to_date(lit(startDateTime.minusYears(6).toString), "yyyy-MM-dd"))
      .selectExpr(columns:_*)

    df
  }
}
