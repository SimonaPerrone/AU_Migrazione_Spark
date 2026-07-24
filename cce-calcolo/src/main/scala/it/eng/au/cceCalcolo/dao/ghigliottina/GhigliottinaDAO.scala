package it.eng.au.cceCalcolo.dao.ghigliottina

import it.eng.au.cceCalcolo.dao.Dao
import it.eng.au.cceCalcolo.schema.ghigliottina.DataGhigliottinaSchema
import it.eng.au.cceCalcolo.utility.environment.Environment
import it.eng.au.cceCalcolo.utility.property.Properties
import org.apache.spark.sql.DataFrame

class GhigliottinaDAO extends Dao {
  override val tablePath: String = Properties.getGhigliottinaTablePath
  override val tableName: String = Properties.getGhigliottinaTableName
  override val columns: List[String] = DataGhigliottinaSchema.getValues

  def getDataGhigliottina: DataFrame = {
    val df = Environment.spark.sqlContext.read.format("csv").option("sep", ";").load(tablePath)
      .withColumnRenamed("_c0", DataGhigliottinaSchema.annomese_aggregato)
      .withColumnRenamed("_c1", DataGhigliottinaSchema.d_ghigliottina)
      .selectExpr(columns:_*)
    df
  }
}
