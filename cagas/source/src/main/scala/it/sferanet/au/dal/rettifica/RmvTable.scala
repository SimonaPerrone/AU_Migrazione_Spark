package it.sferanet.au.dal.rettifica

import it.sferanet.au.dal.rettifica.RmvTable._
import it.sferanet.au.model.rettifica.Rmv
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class RmvTable(inputPath: String) extends Serializable {

  def get(): RDD[Rmv] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        Rmv(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, format, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getOptionDouble(f.measure, r), //segnMis
          getOptionDouble(f.converted, r), //segnConv
          getOptionString(f.serialNumberMis, r), //matricola misuratore
          getOptionString(f.serialNumberConv, r), //matricola convertitore
          getOptionString(collected, r), //raccolta
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

  def filterValidRows(rdd: RDD[Rmv]): RDD[Rmv] = {
    rdd
      .filter(r => Set(1, 2, 3, 4, 5).contains(r.motivation.getOrElse(-1)))
  }
}

object RmvTable {

  def apply(inputPath: String): RmvTable = new RmvTable(inputPath)

  val flowFieldsOldRoute: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_comp",
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
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy(date = "data_prest")
  val motRettLett = "mot_rett_lett"
  val collected = "tipo_rettifica"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def format = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(motRettLett, newRouteField, collected, partitioningColumn))

}
