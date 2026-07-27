package it.sferanet.au.dal.rettifica

import it.sferanet.au.dal.rettifica.RmlTable._
import it.sferanet.au.model.rettifica.Rml
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class RmlTable(inputPath: String) extends Serializable {


  def get(): RDD[Rml] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        Rml(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, format, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getOptionDouble(f.measure, r), //segnMis
          getOptionDouble(f.converted, r), //segnConv
          getOptionString(f.serialNumberMis, r), //matricola misuratore
          getOptionString(f.serialNumberConv, r), //matricola convertitore
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

  def filterValidRows(rdd: RDD[Rml]): RDD[Rml] = {
    rdd
      .filter(r => Set(1, 2, 3, 4, 5, 6).contains(r.motivation.getOrElse(-1)))
  }
}

object RmlTable {

  def apply(inputPath: String): RmlTable = new RmlTable(inputPath)

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
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy()
  val motRettLett = "mot_rett_lett"
  val raccolta = "tipo_rettifica"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def format = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues :::
    List(raccolta, newRouteField, motRettLett, partitioningColumn))
}
