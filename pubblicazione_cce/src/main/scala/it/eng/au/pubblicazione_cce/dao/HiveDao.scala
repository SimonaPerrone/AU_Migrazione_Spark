package it.eng.au.pubblicazione_cce.dao

import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.reflect.runtime.universe.TypeTag

abstract class HiveDao[T <: Product : TypeTag] extends Serializable {
  private val spark = Environment.getSpark

  import spark.implicits._

  val tableName: String

  def columns: List[String]

  def read(): Dataset[T] = {
    readDF()
      .selectExpr(columns: _*)
      .as[T]
  }

  def readDF(): DataFrame = {
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