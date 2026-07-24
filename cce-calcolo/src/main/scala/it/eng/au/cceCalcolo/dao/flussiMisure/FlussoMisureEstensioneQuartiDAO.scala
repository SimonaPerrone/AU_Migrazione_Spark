package it.eng.au.cceCalcolo.dao.flussiMisure

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.flussiMisure.FlussoMisureEstensioneQuartiSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame

class FlussoMisureEstensioneQuartiDAO extends Dao {
  override val tablePath: String = Properties.getFlussoMisureEstensioneQuartiTablePath
  override val tableName: String = Properties.getFlussoMisureEstensioneQuartiTableName
  override val columns: List[String] = FlussoMisureEstensioneQuartiSchema.getValues

  def get(annoQuarti: Int, meseQuarti: Int, monthlyFlag: Boolean): DataFrame = {

    val parquetPath = if (!monthlyFlag) {
      s"$tablePath/annoquarti=$annoQuarti"
    }
    else s"$tablePath/annoquarti=$annoQuarti/mesequarti=$meseQuarti"

    val df = Environment.spark.sqlContext.read.format("parquet").load(parquetPath)
      .withColumnRenamed("podquarti", FlussoMisureEstensioneQuartiSchema.podquarti_mot3)
      .withColumnRenamed("time_stamp", FlussoMisureEstensioneQuartiSchema.time_stamp_mot3)
      .withColumnRenamed("data_misura", FlussoMisureEstensioneQuartiSchema.data_misura_mot3)
      .withColumnRenamed("motivazione", FlussoMisureEstensioneQuartiSchema.motivazione_mot3)
      .withColumnRenamed("pivadistributorequarti", FlussoMisureEstensioneQuartiSchema.pivadistributorequarti_mot3)
      .withColumnRenamed("codcontrdispquarti", FlussoMisureEstensioneQuartiSchema.codcontrdispquarti_mot3)
      .withColumnRenamed("areaquarti", FlussoMisureEstensioneQuartiSchema.areaquarti_mot3)
      .selectExpr(columns:_*)

    df
  }
}
