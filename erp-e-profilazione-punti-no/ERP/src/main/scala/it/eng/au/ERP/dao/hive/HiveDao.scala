package it.eng.au.ERP.dao.hive

import it.eng.au.ERP.schema.SchemaEnum
import it.eng.au.ERP.utility.environment.Environment
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.runtime.universe.TypeTag

abstract class HiveDao[T <: Product: TypeTag]
//(implicit val encoder: Encoder[T])
{
  val tableName: String
  val schema: SchemaEnum
  def columns: List[String] = schema.getValues

  def read(implicit spark: SparkSession,columnsPassed: List[String] = columns): Dataset[T] = {
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .selectExpr(columnsPassed: _*)
      .as[T]
  }

  def read(): Dataset[T] = {
    val spark = Environment.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .selectExpr(columns: _*)
      .as[T]
  }

  def write(data: Dataset[T], overwrite: Boolean = false): Unit = {
    if (overwrite)
      data.write.mode("OVERWRITE").insertInto(tableName)
    else
      data.write.insertInto(tableName)
  }

  def registerTempTable(data: Dataset[T], viewName: String)(implicit spark: SparkSession): Dataset[T] = {
    data.createOrReplaceTempView(viewName)

    data
  }

}

