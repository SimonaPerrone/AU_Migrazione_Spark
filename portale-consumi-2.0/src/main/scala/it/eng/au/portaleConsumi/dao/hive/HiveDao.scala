package it.eng.au.portaleConsumi.dao.hive

import it.eng.au.portaleConsumi.schema.SchemaEnum
import it.eng.au.portaleConsumi.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.TypeTag

abstract class HiveDao[T <: Product: TypeTag]{
  val tableName: String
  val schema: SchemaEnum
  def columns: List[String] = schema.getValues

  def read(columns: List[String] = columns): Dataset[T] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .selectExpr(columns: _*)
      .as[T]
  }

  def readTable(): DataFrame = {
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
