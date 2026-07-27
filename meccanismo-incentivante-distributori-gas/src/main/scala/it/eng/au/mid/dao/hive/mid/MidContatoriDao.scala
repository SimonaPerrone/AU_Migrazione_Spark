package it.eng.au.mid.dao.hive.mid

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.hive.mid.MidContatoriSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{Dataset, SparkSession}

class MidContatoriDao extends HiveDao[MidContatoriModel] {
  override val tableName: String = Environment.getProperty("hive.table.mid_contatori")
  override val schema: SchemaEnum = MidContatoriSchema

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Leggi anomali su precedente calcolo MID per lista annomese
   */
  def leggiAnomaliMidPrecedenti(annomeseLista: List[String]): Dataset[MidContatoriModel] = {
    val rowNumberCol = "_tmp_rn"
    read()
      .where(col(MidContatoriSchema.annomese).isin(annomeseLista: _*))
      .withColumn(rowNumberCol, row_number().over(Window.partitionBy(MidContatoriSchema.pdr, MidContatoriSchema.annomese)
        .orderBy(col(MidContatoriSchema.executionid_tracciatura).desc)))
      .where(col(rowNumberCol) === 1)
      .selectExpr(MidContatoriSchema.getValues: _*)
      .as[MidContatoriModel]
  }
}
