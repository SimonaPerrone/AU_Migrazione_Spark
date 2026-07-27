package it.eng.au.ERP.trasformations

import it.eng.au.ERP.model.au.podConnessioneModel
import it.eng.au.ERP.schema.au.{aggregazioniMisureQuartorarieSchema, podConnessioneSchema, vAggreagazioneMisureIPSchema}
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.functions.Constants
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CalcoloPrelevatoPuntiPrelievoOrari {

  def podConnessioneDataExtraction(dsPodConnesione:Dataset[podConnessioneModel],terna: Boolean,currentPartitions:Int)
                                  (implicit spark: SparkSession) : DataFrame = {

    //CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (TERNA)
    if (terna) {
      return dsPodConnesione
        .repartition(currentPartitions,col(podConnessioneSchema.pod14))
        .filter(col(podConnessioneSchema.t_connessione) =!= Constants.t_connessione_N)
        .select(podConnessioneSchema.pod14)
    }
      //CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (DIST)
    else {
      return dsPodConnesione
        .repartition(currentPartitions,col(podConnessioneSchema.pod14))
        .filter(col(podConnessioneSchema.t_connessione) === Constants.t_connessione_N)
        .select(podConnessioneSchema.pod14)
    }
  }

  def aggregazioniMisureQuartorarieFiltered(dsAggregazioniMisureQuartorarieFiletedAnnoMese : Dataset[Row],
                                            year:Option[Int],
                                            month:Option[Int],
                                            area: Option[String],
                                            singola_piva_distributore: Option[String]
                                            )
                                           (implicit spark: SparkSession) : DataFrame = {

    val dfFilteredYear = year match {
      case Some(yearValue) => dsAggregazioniMisureQuartorarieFiletedAnnoMese
        .filter(col(aggregazioniMisureQuartorarieSchema.anno) === yearValue)
      case None        => dsAggregazioniMisureQuartorarieFiletedAnnoMese
    }

    val dfFilteredMonth = month match {
      case Some(monthValue) => dfFilteredYear
        .filter(col(aggregazioniMisureQuartorarieSchema.mese) === monthValue)
      case None        => dfFilteredYear
    }


    val dsFilterArea = area match {
      case Some(areaValue) => dfFilteredMonth
                        .filter(col(aggregazioniMisureQuartorarieSchema.area) === areaValue)
      case None        => dfFilteredMonth
    }

    val dsFilterPiva = singola_piva_distributore match {
      case Some(piva) => dsFilterArea
        .filter(col(aggregazioniMisureQuartorarieSchema.pivadistributore) === piva)
      case None        => dsFilterArea
    }

    dsFilterPiva
  }

}
