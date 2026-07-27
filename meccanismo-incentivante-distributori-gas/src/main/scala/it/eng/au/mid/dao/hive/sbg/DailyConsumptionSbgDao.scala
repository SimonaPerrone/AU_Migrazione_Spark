package it.eng.au.mid.dao.hive.sbg

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.flow.calcolo.CalcoloMidFunzioni
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionSchema
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import scala.collection.immutable.HashMap

class DailyConsumptionSbgDao extends HiveDao[DailyConsumptionModel] {
  override val tableName: String = Environment.getProperty("hive.table.sbg_daily_consumption")
  override val schema: SchemaEnum = DailyConsumptionSchema

  override def read(): Dataset[DailyConsumptionModel] = {
    val spark = Environment.getSpark
    import spark.implicits._

    readDF()
      .where(col(DailyConsumptionSchema.session) === "SBG")
      .selectExpr(columns: _*)
      .as[DailyConsumptionModel]
  }

  /** *
   * Ritorna annomese, sessione, tracciatura, executionId dell'utlima esecuzione
   */
  def infoUltimaEsecuzione(annomese: String): HashMap[String, Any] = {
    val sbg = read()
      .select(
        DailyConsumptionSchema.annomese,
        DailyConsumptionSchema.executionid,
        DailyConsumptionSchema.session
      )
    val maxExecutionId = CalcoloMidFunzioni.maxExecutionIdPerAnnomese(sbg, annomese) match {
      case Some(value) => value
      case None => throw new Exception(s"Nessuna dato trovato per $annomese")
    }

    val ultimaEsecuzione = sbg
      .where(col(DailyConsumptionSchema.executionid) === maxExecutionId)
      .select(
        DailyConsumptionSchema.annomese,
        DailyConsumptionSchema.session
      )
      .limit(1)
      .head()

    HashMap(
      "annomese" -> ultimaEsecuzione.getAs[String](DailyConsumptionSchema.annomese),
      "sessione" -> "SBG",
      "tracciatura" -> "SBG",
      "executionId" -> maxExecutionId
    )
  }
}
