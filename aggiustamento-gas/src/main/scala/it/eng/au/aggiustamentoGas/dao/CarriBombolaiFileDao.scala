package it.eng.au.aggiustamentoGas.dao

import it.eng.au.aggiustamentoGas.schema.CarriBombolaiFileSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class CarriBombolaiFileDao extends Dao {
  override val parquetPath: String = Environment.getCarriBombolaiFilePath
  override val columns: List[String] = CarriBombolaiFileSchema.getValues

  def get: DataFrame = {
    Environment.getSpark.read
      .format("csv")
      .option("header", "false")
      .option("sep", ";")
      .option("inferSchema", "true")
      .load(parquetPath)
      .toDF(columns:_*)
      .withColumn(CarriBombolaiFileSchema.carriBombolaiFlag, lit(true))
      .selectExpr(columns:_*)
  }
}
