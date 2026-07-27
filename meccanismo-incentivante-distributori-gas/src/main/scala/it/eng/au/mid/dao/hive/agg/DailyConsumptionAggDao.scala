package it.eng.au.mid.dao.hive.agg

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionSchema
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, collect_set, max}

import scala.collection.immutable.HashMap

class DailyConsumptionAggDao extends HiveDao[DailyConsumptionModel] {
  override val tableName: String = Environment.getProperty("hive.table.agg_daily_consumption")
  override val schema: SchemaEnum = DailyConsumptionSchema

  override def read(): Dataset[DailyConsumptionModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readDF()
      .where(col(DailyConsumptionSchema.session).contains("AGG"))
      .selectExpr(columns: _*)
      .as[DailyConsumptionModel]
  }

  /** *
   * Ritorna annomese, sessione, tracciatura, executionId dell'utlima esecuzione
   */
  def infoUltimaEsecuzione(executionId: Long): HashMap[String, Any] = {
    val agg = readDF()
      .where(col(DailyConsumptionSchema.executionid) === executionId)
      .groupBy(col(DailyConsumptionSchema.executionid))
      .agg(
        collect_set(col(DailyConsumptionSchema.annomese)).as("annomeseAnomali"),
        max(col(DailyConsumptionSchema.session)).as(DailyConsumptionSchema.session)
      )
      .cache()

    if (agg.isEmpty) {
      throw new Exception(s"Errore: $tableName non contiene dati ${DailyConsumptionSchema.executionid} uguale a $executionId")
    }
    val res = agg.head()

    HashMap(
      "annomeseAnomali" -> res.getAs[String]("annomeseAnomali"),
      "sessione" -> "AGG",
      "tracciatura" -> res.getAs[String](DailyConsumptionSchema.session),
      "executionId" -> executionId
    )
  }
}
