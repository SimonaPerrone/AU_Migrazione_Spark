package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.Sm1Table._
import it.sferanet.au.model.prestazionale.Sm1
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class Sm1Table(inputPath: String) extends Serializable {

  def get(): RDD[Sm1] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        Sm1(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r).toString, //cod_pdr,
          getOptionChar(readType, r), //tipoLettura
          getOptionDate(f.date, format, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getEsito(outcome, r), //esito
          getOptionDouble(f.measure, r), //segnMisSost
          getOptionDouble(f.converted, r), //segnConv
          getOptionString(f.serialNumberMis, r),
          getOptionString(f.serialNumberConv, r),
          getOptionString(f.local_file, r),
          getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
          getOptionString(f.ammissibilita, r),
          isNewRouteVersion
        )
      }
      )
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[Sm1]): RDD[Sm1] = {
    rdd
      .filter(r => (r.ammissibilita.isEmpty && r.outcome.getOrElse('-') == '1' && r.readType.getOrElse('-') == 'E') ||
        (r.ammissibilita.isDefined && Set('E', 'A', 'S').contains(r.readType.getOrElse('-'))))
  }
}

object Sm1Table {

  def apply(inputPath: String): Sm1Table = new Sm1Table(inputPath)

  val flowFieldsOldRoute: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_ril",
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
  val readType = "tipo_lettura"
  val newRouteField = "ammissibilita"
  val outcome = "esito"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues :::
    List(outcome, newRouteField, readType, partitioningColumn))

  def format = Constants.STANDARD_FORMAT_DATE


}






