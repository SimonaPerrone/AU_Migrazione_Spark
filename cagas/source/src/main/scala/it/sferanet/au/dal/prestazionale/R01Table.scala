package it.sferanet.au.dal.prestazionale


import it.sferanet.au.dal.prestazionale.R01Table._
import it.sferanet.au.model.prestazionale.R01
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class R01Table(inputPath: String) extends Serializable {

  def get(): RDD[R01] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        R01(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionChar(readType, r), //tipoLettura
          getOptionDate(f.date, format, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getEsito(outcome, r), //esito
          getOptionDouble(f.measure, r), //segnMis
          getOptionDouble(f.converted, r), //segnConv
          getOptionString(f.serialNumberMis, r), //matricola misuratore
          getOptionString(f.serialNumberConv, r), //matricola convertitore
          getOptionString(collected, r), //raccolta
          getOptionString(f.local_file, r), //local file
          getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
          getOptionString(f.ammissibilita, r),
          isNewRouteVersion
        )
      })
      // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[R01]): RDD[R01] = {
    rdd
      .filter(r => (r.ammissibilita.isEmpty && r.outcome.getOrElse('-') == '1') ||
        (r.ammissibilita.isDefined && Set('E', 'A', 'S').contains(r.readType.getOrElse('-'))))
  }
}

object R01Table {

  def apply(inputPath: String): R01Table = new R01Table(inputPath)

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
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy(date = "data_prest",
    measure = "let_tot_prel",
    converted = "let_tot_conv")
  val readType = "tipo_lettura"
  val collected = "raccolta"
  val newRouteField = "ammissibilita"
  val outcome = "esito"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(outcome, readType, newRouteField, collected, partitioningColumn))

  def format = Constants.STANDARD_FORMAT_DATE

}




