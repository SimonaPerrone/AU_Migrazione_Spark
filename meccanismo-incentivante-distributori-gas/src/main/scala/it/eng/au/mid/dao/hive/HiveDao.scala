package it.eng.au.mid.dao.hive

import it.eng.au.mid.environment.Environment
import it.eng.au.mid.schema.SchemaEnum
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.TypeTag

abstract class HiveDao[T <: Product : TypeTag] {
  val tableName: String
  val schema: SchemaEnum

  def columns: List[String] = schema.getValues

  def read(): Dataset[T] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .selectExpr(columns: _*)
      .as[T]
  }

  def readDF(): DataFrame = {
    val spark = Environment.getSpark
    spark.sqlContext.read
      .table(tableName)
  }


  def write(data: Dataset[T], overwrite: Boolean = false): Unit = {
    if (overwrite)
      data.write.mode("OVERWRITE").insertInto(tableName)
    else
      data.write.insertInto(tableName)
  }

}
