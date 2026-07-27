package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.TmvTable._
import it.sferanet.au.model.prestazionale.Tmv
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import java.text.SimpleDateFormat

class TmvTable(inputPath: String) extends Serializable {

  def get(): RDD[Tmv] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        Tmv(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, formatDate, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getOptionChar(TmvTable.readType, r), //tipoLettura
          getOptionDouble(f.measure, r), //segn_mis_sost
          getOptionDouble(f.converted, r), //segn_conv
          getOptionString(f.serialNumberMis, r),
          getOptionString(f.serialNumberConv, r),
          getOptionString(f.local_file, r),
          getOptionDate(f.d_caricamento, formatDCaricamento, r),
          getOptionString(f.ammissibilita, r),
          isNewRouteVersion
        )
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[Tmv]): RDD[Tmv] = {
    rdd
      .filter(r =>
        (r.ammissibilita.isEmpty && Set('E').contains(r.readType.getOrElse('-'))) ||
          (r.ammissibilita.isDefined && Set('E', 'A').contains(r.readType.getOrElse('-'))))
  }
}

object TmvTable {
  def apply(inputPath: String): TmvTable = new TmvTable(inputPath)

  val flowFieldsOldRoute: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_att_contr",
    "piva_distr",
    "piva_utente",
    "segn_mis_sost",
    "segn_conv",
    "matr_mis",
    "matr_conv",
    "local_file",
    "d_caricamento",
    "ammissibilita"
  )
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy(date = "data_prest", measure = "let_tot_prel", converted = "let_tot_conv")

  /** */
  val readType = "tipo_lettura"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  val schema: StructType = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(readType, newRouteField, partitioningColumn))

  def formatDate: SimpleDateFormat = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento: SimpleDateFormat = Constants.FORMAT_DATE_LOAD
}
