package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.A01Table._
import it.sferanet.au.model.prestazionale.A01
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class A01Table(inputPath: String) extends Serializable {

  def get(): RDD[A01] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        A01(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionChar(A01Table.readType, r),
          getOptionDate(f.date, format, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getEsito(outcome, r), //esito
          getOptionDouble(f.measure, r), //segnMis
          getOptionDouble(f.converted, r), //segnConv
          getOptionString(f.serialNumberMis, r), //matricola misuratore
          getOptionString(f.serialNumberConv, r), //matricola convertitore
          getOptionString(collected, r),
          getOptionString(f.local_file, r), //local file
          getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
          getOptionString(f.ammissibilita, r),
          isNewRouteVersion
        )
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[A01]): RDD[A01] = {
    rdd
      .filter(r => (r.ammissibilita.isEmpty && r.outcome.getOrElse('-') == '1') ||
        (r.ammissibilita.isDefined && Set('E', 'A','S').contains(r.readType.getOrElse('-'))))
  }
}

object A01Table {

  def apply(inputPath: String): A01Table = new A01Table(inputPath)

  val flowFieldsOldRoute: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_attivazione",
    "piva_distr",
    "piva_utente",
    "segn_mis",
    "segn_conv",
    "matr_mis",
    "matr_conv",
    "local_file",
    "d_caricamento",
    "ammissibilita"
  )
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy(date = "data_prest", measure = "let_tot_prel", converted = "let_tot_conv")
  val outcome = "esito"
  val newRouteField = "ammissibilita"
  val readType = "tipo_lettura"
  val collected = "raccolta"
  val partitioningColumn = "annomese"

  def format = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(collected, readType, outcome, newRouteField, partitioningColumn))

}
