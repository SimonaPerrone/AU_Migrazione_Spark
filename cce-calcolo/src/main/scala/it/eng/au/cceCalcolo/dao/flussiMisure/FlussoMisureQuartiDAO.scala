package it.eng.au.cceCalcolo.dao.flussiMisure

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.flussiMisure.FlussoMisureQuartiSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class FlussoMisureQuartiDAO extends Dao {
  override val tablePath: String = Properties.getFlussoMisureQuartiTablePath
  override val tableName: String = Properties.getFlussoMisureQuartiTableName
  override val columns: List[String] = FlussoMisureQuartiSchema.getValues

  def get(annoQuarti: Int, meseQuarti: Int, monthlyFlag: Boolean): DataFrame = {

    val parquetPath = if (!monthlyFlag) {
      s"$tablePath/annoquarti=$annoQuarti"
    }
    else s"$tablePath/annoquarti=$annoQuarti/mesequarti=$meseQuarti"

    val df = if (!monthlyFlag) {
      Environment.spark.sqlContext.read.format("parquet").load(parquetPath)
        .withColumn(FlussoMisureQuartiSchema.annoquarti, lit(annoQuarti))
        .selectExpr(columns:_*)
    }
    else
      Environment.spark.sqlContext.read.format("parquet").load(parquetPath)
        .withColumn(FlussoMisureQuartiSchema.annoquarti, lit(annoQuarti))
        .withColumn(FlussoMisureQuartiSchema.mesequarti, lit(meseQuarti))
        .selectExpr(columns: _*)

    df
  }
}
