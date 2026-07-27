package it.eng.au.portale_consumi_ee.dao

import it.eng.au.portale_consumi_ee.common.schema.SchemaEnum
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.schema.misure.MisureStoricF2Schema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

abstract class HiveMisureDao[T <: Product: TypeTag]
//(implicit val encoder: Encoder[T])
{
  val tableName: String
  val schema: SchemaEnum
  def columns: List[String] = schema.getValues

  def read(implicit spark: SparkSession,columns: List[String] = columns): Dataset[T] = {

    val spark = EnvironmentMisure.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .selectExpr(columns: _*)
      .as[T]
  }

  def read(): Dataset[T] = {
    val spark = EnvironmentMisure.getSpark
    import spark.implicits._

    spark.sqlContext.read
      .table(tableName)
      .selectExpr(columns: _*)
      .as[T]
  }

  def read(storico:Boolean,annomese:Int): Dataset[T] = {
    val spark = EnvironmentMisure.getSpark
    import spark.implicits._

    val competenza_consumi = "competenza_consumi"

    if(storico) {
      spark.sqlContext.read
        .table(tableName)
        .filter(col(competenza_consumi) <= annomese)
        .selectExpr(columns: _*)
        .as[T]
    } else
      {
        spark.sqlContext.read
          .table(tableName)
          .filter(col(competenza_consumi) > annomese)
          .selectExpr(columns: _*)
          .as[T]
      }
  }

  def readWindowStoric(storico:Boolean,listAnnomese:List[Int]): Dataset[T] = {
    val spark = EnvironmentMisure.getSpark
    import spark.implicits._
    val competenza_consumi= "competenza_consumi"
    spark.sqlContext.read
      .table(tableName)
      .filter(col(competenza_consumi)isin(listAnnomese:_*))
      .selectExpr(columns: _*)
      .as[T]

  }

  def write(data: Dataset[T], overwrite: Boolean = false): Unit = {
    if (overwrite) {

      data
        .write
        .mode("OVERWRITE")
        .insertInto(tableName)
    } else
      data.write.insertInto(tableName)
  }

}

