package it.sferanet.au.dal.rettifica

import it.sferanet.au.dal.rettifica.RglTable._
import it.sferanet.au.model.rettifica.Rgl
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class RglTable(inputPath: String) extends Serializable {


  def get(): RDD[Rgl] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        val format = if (isNewRouteVersion) formatNewRoute else formatOldRoute
        Rgl(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, format, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getOptionDouble(f.measure, r), //segnMis
          getOptionDouble(f.converted, r), //segnConv
          getSerialNumberMisMisureGiornaliere(r, flowFieldsNewRoute, flowFieldsOldRoute), //matricola misuratore
          getSerialNumberConvMisureGiornaliere(r, flowFieldsNewRoute, flowFieldsOldRoute), //matricola convertitore
          getOptionString(raccolta, r), //raccolta
          getOptionInt(motRettLett, r), //mottRettLett
          getOptionString(f.local_file, r),
          getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
          getOptionString(f.ammissibilita, r),
          isNewRouteVersion
        )
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[Rgl]): RDD[Rgl] = {
    rdd
      .filter(r => (r.ammissibilita.isEmpty && Set(1, 2, 3, 4, 5, 6).contains(r.motivation.getOrElse(-1))) ||
        (r.ammissibilita.isDefined && Set(1, 2, 3, 4, 5, 6, 7).contains(r.motivation.getOrElse(-1))))
  }
}

object RglTable {

  def apply(inputPath: String): RglTable = new RglTable(inputPath)

  val flowFieldsOldRoute: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_racc",
    "piva_distr",
    "piva_utente",
    "let_tot_prel",
    "let_tot_conv",
    "matr_mis",
    "matr_conv",
    "local_file",
    "d_caricamento",
    "ammissibilita"
  )
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy(serialNumberMis = "matr_mis_giornaliere", serialNumberConv = "matr_conv_giornaliere")
  val motRettLett = "mot_rett_lett"
  val raccolta = "tipo_rettifica"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "mese_comp"

  def formatOldRoute = Constants.getFormatter("yyyy-MM-dd") //formato diverso dallo standard degli altri flussi

  def formatNewRoute = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(raccolta, newRouteField, motRettLett, partitioningColumn))

}
