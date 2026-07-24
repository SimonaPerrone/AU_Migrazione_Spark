package it.eng.au.cceCalcolo.dao.rcus

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.rcus.RcusPodMisurePSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, to_date, year}
import org.apache.spark.sql.DataFrame

class RcusPodMisurePDAO extends Dao {
  override val tablePath: String = Properties.getRcusPodMisurePTablePath
  override val tableName: String = Properties.getRcusPodMisurePTableName
  override val columns: List[String] = RcusPodMisurePSchema.getValues
  val annoMeseCol = "anno_mese_in"

  def getRcusPodMisureP(annoIn: Int, meseIn: Int, meseInput: Option[Int]): DataFrame = {
    val df =
      if (meseInput.isEmpty)
        Environment.spark.read.parquet(tablePath)
          .filter(year(to_date(col(RcusPodMisurePSchema.d_anno_mese), "yyyy-MM-dd")) === annoIn)
          .selectExpr(columns:_*)
      else
        Environment.spark.read.parquet(tablePath)
          .withColumn(annoMeseCol, to_date(concat_ws("-", lit(annoIn), lit(meseIn), lit("01"))))
          .filter(to_date(col(RcusPodMisurePSchema.d_anno_mese), "yyyy-MM-dd") <= col(annoMeseCol))
          .selectExpr(columns:_*)

    df
  }
}
