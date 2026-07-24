package it.eng.au.cceCalcolo.dao.flussiMisure

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.flussiMisure.FlussoMisureQuartiInSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class FlussoMisureQuartiInDAO extends Dao {
  override val tablePath: String = Properties.getFlussoMisureQuartiInTablePath
  override val tableName: String = Properties.getFlussoMisureQuartiInTableName
  override val columns: List[String] = FlussoMisureQuartiInSchema.getValues

  def get(annoQuarti: Int, meseQuarti: Int, monthlyFlag: Boolean): DataFrame = {

    val parquetPath = if (!monthlyFlag) {
      s"$tablePath/annoquarti=$annoQuarti"
    }
    else s"$tablePath/annoquarti=$annoQuarti/mesequarti=$meseQuarti"

    val df = if (!monthlyFlag) {
      Environment.spark.sqlContext.read.format("parquet").load(parquetPath)
        .withColumn(FlussoMisureQuartiInSchema.annoquarti, lit(annoQuarti))
        .selectExpr(columns: _*)
    }
    else
      Environment.spark.sqlContext.read.format("parquet").load(parquetPath)
        .withColumn(FlussoMisureQuartiInSchema.annoquarti, lit(annoQuarti))
        .withColumn(FlussoMisureQuartiInSchema.mesequarti, lit(meseQuarti))
        .selectExpr(columns: _*)

    df
  }
}

