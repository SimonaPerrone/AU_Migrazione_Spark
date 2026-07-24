package it.eng.au.cceCalcolo.dao.rcus

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcus.RcusPodUddPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.environment.Environment.startDateTime
import it.eng.au.cceCalcolo.utility.parsedate.DateUtility.fillNullDates
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_date}

class RcusPodUddPDAO extends Dao {
  override val tablePath: String = Properties.getRcusPodUddPTablePath
  override val tableName: String = Properties.getRcusPodUddPTableName
  override val columns: List[String] = RcusPodUddPSchema.getValues

  def getRcusPodUddP: DataFrame = {
    val df = fillNullDates(Environment.spark.read.parquet(tablePath), RcusPodUddPSchema.d_inizio, RcusPodUddPSchema.d_fine)
      .filter(col(RcusPodUddPSchema.d_inizio) <= col(RcusPodUddPSchema.d_fine))
      .filter(col(RcusPodUddPSchema.b_valido) === "N")
      .filter(col(RcusPodUddPSchema.d_fine) >= to_date(lit(startDateTime.minusYears(6).toString), "yyyy-MM-dd"))
      .selectExpr(columns:_*)

    df
  }
}
