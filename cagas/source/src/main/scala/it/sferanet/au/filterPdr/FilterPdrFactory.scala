package it.sferanet.au.filterPdr

import it.sferanet.au.model.Flow
import it.sferanet.au.utilities.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object FilterPdrFactory {
  private val log = org.apache.log4j.LogManager.getLogger(this.getClass)

  // Di tutti questi filtri negli ultimi 2 anni è stato utilizzato soltanto il FILTER_MODE_PERIMETRO_AGG_RIC (oltre alla modalità massiva)
  def getLaunchMode(mode: String, measures: RDD[Flow], pdrMassivo: DataFrame): (RDD[Flow], DataFrame) = {
    log.info(s"filter pdr mode: $mode")
    val filter = mode match {
      case Constants.FILTER_MODE_PDR => Some(new Filter1List)
      case Constants.FILTER_MODE_DISTRIBUTORE => Some(new Filter2Distributore)
      case Constants.FILTER_MODE_DISTRIBUTORE_UDD => Some(new Filter3UDD)
      case Constants.FILTER_MODE_DISTRIBUTORE_UDD_UDB => Some(new Filter4UDB)
      case Constants.FILTER_MODE_OGGETTO_VARIAZIONE => Some(new Filter51EventOggettoVariazione)
      case Constants.FILTER_MODE_CARICAMENTO_TDS => Some(new Filter52EventCaricamentoTds)
      case Constants.FILTER_MODE_NO_MISURE => Some(new Filter53EventNoMisure)
      case Constants.FILTER_MODE_FORZATURA => Some(new Filter6CalculateCaForcing)
      case Constants.FILTER_MODE_PERIMETRO_AGG_RIC => Some(new FilterPerimetroAggRic(pdrMassivo))
      case _ =>
        log.info("massive launch")
        None
    }

    if (filter.isDefined) {
      (filter.get.filter(measures), filter.get.filterPdrMassivo(pdrMassivo))
    } else {
      (measures, pdrMassivo)
    }
  }
}
