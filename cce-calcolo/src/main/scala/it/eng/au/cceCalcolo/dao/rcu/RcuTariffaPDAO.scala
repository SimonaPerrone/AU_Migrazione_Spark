package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuTariffaPSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.parsedate.DateUtility.fillNullDates
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class RcuTariffaPDAO extends Dao {
  override val tablePath: String = Properties.getRcuTariffaPTablePath
  override val tableName: String = Properties.getRcuTariffaPTableName
  override val columns: List[String] = RcuTariffaPSchema.getValues

  def getRcuTariffaP: DataFrame = {
    val df = fillNullDates(Environment.spark.read.parquet(tablePath), RcuTariffaPSchema.d_inizio_tariffa, RcuTariffaPSchema.d_fine_tariffa)
      .filter(
          (col(RcuTariffaPSchema.b_valido)==="N") and (col(RcuTariffaPSchema.b_ultima)==="N")
        or
          (col(RcuTariffaPSchema.b_valido)==="Y") and (col(RcuTariffaPSchema.b_ultima)==="Y")
      )
      .selectExpr(columns:_*)

    df
  }
}
