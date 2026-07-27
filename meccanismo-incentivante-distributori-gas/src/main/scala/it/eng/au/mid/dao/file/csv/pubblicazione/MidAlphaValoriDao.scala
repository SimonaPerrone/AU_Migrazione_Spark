package it.eng.au.mid.dao.file.csv.pubblicazione

import it.eng.au.mid.dao.file.csv.CsvDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.pubblicazione.MidAlphaValoriModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.file.pubblicazione.MidAlphaValoriSchema
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

class MidAlphaValoriDao extends CsvDao[MidAlphaValoriModel] {
  override val path: String = Environment.getProperty("file.path.mid_alpha_valori")
  override val schema: SchemaEnum = MidAlphaValoriSchema

  override def read(): Dataset[MidAlphaValoriModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .option("header", header)
      .option("delimiter", delimiter)
      .option("inferSchema", inferSchema)
      .csv(path)
      .where(col(MidAlphaValoriSchema.gdm).isNotNull)
      .withColumn(MidAlphaValoriSchema.alpha, col(MidAlphaValoriSchema.alpha).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[MidAlphaValoriModel]
  }
}
