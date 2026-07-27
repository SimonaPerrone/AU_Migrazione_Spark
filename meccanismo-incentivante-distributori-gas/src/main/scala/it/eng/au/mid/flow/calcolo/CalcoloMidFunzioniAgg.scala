package it.eng.au.mid.flow.calcolo

import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.flow.calcolo.{DailyConsumptionEsclusiModel, DailyConsumptionIncoerentiModel, PdrAnomaloModel}
import it.eng.au.mid.schema.flow.calcolo.{DailyConsumptionEsclusiSchema, DailyConsumptionIncoerentiSchema, PdrAnomaloSchema}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Dataset, SparkSession}

/***
 * Funzioni in comune tra i processi AGG standard e back in time
 */
object CalcoloMidFunzioniAgg {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /** *
   * Legge PDR anomali da tabelle input [[incoerentiDao]] e [[esclusiDao]] per [[executionId]] passato
   */
  def leggiPdrAnomaliAgg(incoerentiDs: Dataset[DailyConsumptionIncoerentiModel],
                         esclusiDs: Dataset[DailyConsumptionEsclusiModel],
                         executionId: Long, annomeseLista: List[String]): Dataset[PdrAnomaloModel] = {
    val incoerentiAnomaliDs = incoerentiDs
      .where(col(DailyConsumptionIncoerentiSchema.executionid) === executionId)
      .where(col(DailyConsumptionIncoerentiSchema.annomese).isin(annomeseLista: _*))
      .where(col(DailyConsumptionIncoerentiSchema.ispdranomalousgdm) === true)
      .select(
        DailyConsumptionIncoerentiSchema.pdr,
        DailyConsumptionIncoerentiSchema.annomese,
        DailyConsumptionIncoerentiSchema.executionid
      )
      .distinct()
      .withColumn(PdrAnomaloSchema.tipoAnomalia, lit(CostantiMid.CAUSALE_INCOERENTI))
      .selectExpr(PdrAnomaloSchema.getValues: _*)
      .as[PdrAnomaloModel]

    val esclusiAnomaliDs = esclusiDs
      .where(col(DailyConsumptionEsclusiSchema.executionid) === executionId)
      .where(col(DailyConsumptionEsclusiSchema.annomese).isin(annomeseLista: _*))
      .select(
        DailyConsumptionEsclusiSchema.pdr,
        DailyConsumptionEsclusiSchema.annomese,
        DailyConsumptionEsclusiSchema.executionid
      )
      .distinct()
      .withColumn(PdrAnomaloSchema.tipoAnomalia, lit(CostantiMid.CAUSALE_ESCLUSI))
      .selectExpr(PdrAnomaloSchema.getValues: _*)
      .as[PdrAnomaloModel]

    incoerentiAnomaliDs
      .union(esclusiAnomaliDs)
  }


}
