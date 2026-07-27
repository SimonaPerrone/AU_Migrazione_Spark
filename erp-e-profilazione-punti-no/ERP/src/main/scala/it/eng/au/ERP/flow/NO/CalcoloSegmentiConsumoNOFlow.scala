package it.eng.au.ERP.flow.NO

import it.eng.au.ERP.dao.hive.erp.{ErpConsumptionNODao, ErpValidatedMisNODao}
import it.eng.au.ERP.flow.Flow
import it.eng.au.ERP.model.erp.erpConsumptionNoModel
import it.eng.au.ERP.schema.erp.{erpConsumptionNoSchema, erpValidatedMisNoSchema}
import it.eng.au.ERP.trasformations.NO.CalcoloSegmentIConsumoNOTrasformation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

case class CalcoloSegmentiConsumoNOFlow()(implicit spark: SparkSession) extends Flow {

  private val erpValidatedMisNODao = new ErpValidatedMisNODao
  private val erpConsumptionNODao = new ErpConsumptionNODao

  def run(timestamp: Long, podExcluded: List[String], annomese: Option[String]): Unit = {
    logger.info("Inizio calcolo segmenti di consumo NO")

    val executionId = timestamp.toString

    val misureValidateDf = erpValidatedMisNODao
      .read()
      .filter(col(erpValidatedMisNoSchema.executionid.toString) === executionId)

    if (misureValidateDf.head(1).isEmpty) {
      logger.warn(s"Nessuna misura validata trovata per executionId $executionId; il calcolo dei segmenti NO viene saltato.")
      return
    }

    val segmentiDf = CalcoloSegmentIConsumoNOTrasformation.buildConsumptionSegments(
      misureValidateDf,
      annomese,
      timestamp,
      podExcluded
    )

    import spark.implicits._
    val segmentiDs: Dataset[erpConsumptionNoModel] = segmentiDf
      .selectExpr(erpConsumptionNoSchema.getValues: _*)
      .as[erpConsumptionNoModel]

    logger.info(s"Inizio scrittura dei segmenti NO su tabella ${erpConsumptionNODao.tableName}")
    erpConsumptionNODao.write(segmentiDs, overwrite = false)
    logger.info("Fine calcolo segmenti di consumo NO")
  }
}
