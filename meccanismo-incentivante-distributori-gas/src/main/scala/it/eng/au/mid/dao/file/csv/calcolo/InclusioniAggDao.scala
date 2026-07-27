package it.eng.au.mid.dao.file.csv.calcolo

import it.eng.au.mid.dao.file.csv.CsvDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.calcolo.InclusioniModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.file.calcolo.InclusioniSchema
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

class InclusioniAggDao extends CsvDao[InclusioniModel] {
  override val path: String = Environment.getProperty("file.path.inclusioni_agg")
  override val schema: SchemaEnum = InclusioniSchema

  override   def read(): Dataset[InclusioniModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .option("header", header)
      .option("delimiter", delimiter)
      .option("inferSchema", inferSchema)
      .csv(path)
      .where(col(InclusioniSchema.pdr).isNotNull)
      .withColumn(InclusioniSchema.n, col(InclusioniSchema.n).cast(IntegerType))
      .selectExpr(columns: _*)
      .as[InclusioniModel]
  }
}
