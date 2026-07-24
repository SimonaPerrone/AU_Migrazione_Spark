package it.eng.au.cceCalcolo.dao.rcus

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcus.RcusFornituraPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.parsedate.DateUtility.fillNullDates
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class RcusFornituraPDAO extends Dao {
  override val tablePath: String = Properties.getRcusFornuturaPTablePath
  override val tableName: String = Properties.getRcusFornuturaPTableName
  override val columns: List[String] = RcusFornituraPSchema.getValues

  def getRcusFornituraP: DataFrame = {
    val df = fillNullDates(Environment.spark.read.parquet(tablePath), RcusFornituraPSchema.d_inizio_titolarita, RcusFornituraPSchema.d_fine_titolarita)
      .filter(col(RcusFornituraPSchema.d_inizio_titolarita) <= col(RcusFornituraPSchema.d_fine_titolarita))
      .filter(col(RcusFornituraPSchema.b_valido) === "N")
      .selectExpr(columns:_*)

    df
  }
}
