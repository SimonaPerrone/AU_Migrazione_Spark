package it.eng.au.cceCalcolo.dao.flussiMisure

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.flussiMisure.FlussoMisureEstensioneQuartiInSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame

class FlussoMisureEstensioneQuartiInDAO extends Dao {
  override val tablePath: String = Properties.getFlussoMisureEstensioneQuartiInTablePath
  override val tableName: String = Properties.getFlussoMisureEstensioneQuartiInTableName
  override val columns: List[String] = FlussoMisureEstensioneQuartiInSchema.getValues

  def get(annoQuarti: Int, meseQuarti: Int, monthlyFlag: Boolean): DataFrame = {

    val parquetPath = if (!monthlyFlag) {
      s"$tablePath/annoquarti=$annoQuarti"
    }
    else s"$tablePath/annoquarti=$annoQuarti/mesequarti=$meseQuarti"

    val df = Environment.spark.sqlContext.read.format("parquet").load(parquetPath)
      .withColumnRenamed("podquarti", FlussoMisureEstensioneQuartiInSchema.podquarti_mot3)
      .withColumnRenamed("time_stamp", FlussoMisureEstensioneQuartiInSchema.time_stamp_mot3)
      .withColumnRenamed("data_misura", FlussoMisureEstensioneQuartiInSchema.data_misura_mot3)
      .withColumnRenamed("motivazione", FlussoMisureEstensioneQuartiInSchema.motivazione_mot3)
      .withColumnRenamed("pivadistributorequarti", FlussoMisureEstensioneQuartiInSchema.pivadistributorequarti_mot3)
      .withColumnRenamed("codcontrdispquarti", FlussoMisureEstensioneQuartiInSchema.codcontrdispquarti_mot3)
      .withColumnRenamed("areaquarti", FlussoMisureEstensioneQuartiInSchema.areaquarti_mot3)
      .selectExpr(columns: _*)

    df
  }
}
