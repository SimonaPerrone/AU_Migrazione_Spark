package it.eng.au.ERP.flow.NO

import it.eng.au.ERP.flow.Flow
import org.apache.spark.sql.SparkSession

case class CalcoloEnergiaImmessaPrelevataNOFlow()(implicit spark: SparkSession) extends Flow {

  def run(timestamp: Long, podExcluded: List[String], annomese: Option[String]): Unit = {

    logger.info("Inizio calcolo prelevato ai punti di prelievo no (non orari)")

    logger.info("Inizio Ingestion flussi di misura e validazione misure")
    CalcoloEnergiaImmessaPrelevataNOIngestionFlow().run(timestamp, podExcluded, annomese)
    logger.info("Fine Ingestion flussi di misura e validazione misure")

    logger.info("Inizio calcolo segmenti consumo NO")
    CalcoloSegmentiConsumoNOFlow().run(timestamp, podExcluded, annomese)
    logger.info("Fine calcolo segmenti consumo NO")

    logger.info("Inizio profilazione curve quartorarie NO")
    CalcoloProfilazioneCurveNOFlow().run(timestamp, podExcluded, annomese)
    logger.info("Fine profilazione curve quartorarie NO")

    logger.info("Inizio aggregazione finale NO")
    CalcoloPrelevatoPuntiPrelievoNOFlow().run(timestamp, podExcluded, annomese)
    logger.info("Fine aggregazione finale NO")

    logger.info("Fine calcolo prelevato ai punti di prelievo no (non orari)")
  }
}