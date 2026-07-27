package it.eng.au.mid.dao.file.csv

import it.eng.au.mid.environment.Environment
import it.eng.au.mid.schema.SchemaEnum
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe.TypeTag

abstract class CsvDao[T <: Product : TypeTag] {
  val delimiter: String = ";"
  val header: Boolean = true
  val inferSchema: Boolean = false

  val path: String
  val schema: SchemaEnum

  def columns: List[String] = schema.getValues

  def read(): Dataset[T] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .option("header", header)
      .option("delimiter", delimiter)
      .option("inferSchema", inferSchema)
      .csv(path)
      .selectExpr(columns: _*)
      .as[T]
  }

}
