package it.eng.au.cceCalcolo.dao.rcu

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcu.RcuPodMisurePSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, lit, to_date, year}

class RcuPodMisurePDAO extends Dao {
  override val tablePath: String = Properties.getRcuPodMisurePTablePath
  override val tableName: String = Properties.getRcuPodMisurePTableName
  override val columns: List[String] = RcuPodMisurePSchema.getValues
  val annoMeseCol = "anno_mese_in"

  def getRcuPodMisureP(annoIn: Int, meseIn: Int, meseInput: Option[Int]): DataFrame = {
    val df =
      if (meseInput.isEmpty)
        Environment.spark.read.parquet(tablePath)
          .filter(year(to_date(col(RcuPodMisurePSchema.d_anno_mese), "yyyy-MM-dd")) === annoIn)
          .selectExpr(columns:_*)
      else
        Environment.spark.read.parquet(tablePath)
          .withColumn(annoMeseCol, to_date(concat_ws("-", lit(annoIn), lit(meseIn), lit("01"))))
          .filter(to_date(col(RcuPodMisurePSchema.d_anno_mese), "yyyy-MM-dd") <= col(annoMeseCol))
          .selectExpr(columns:_*)

    df
  }
}
