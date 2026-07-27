package it.sferanet.au.dal.periodico

import it.sferanet.au.dal.periodico.TmlTable._
import it.sferanet.au.model.periodico.Tml
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import java.text.SimpleDateFormat

class TmlTable(inputPath: String) extends Serializable {

  def get(): RDD[Tml] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, TmlTable.newRouteField)
        val f = if (isNewRouteVersion) TmlTable.flowFieldsNewRoute else TmlTable.flowFieldsOldRoute
        Tml(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, formatDate, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getOptionChar(readType, r), //tipoLettura
          getOptionString(isValid, r), //isValid
          getOptionDouble(f.measure, r), //letTotPrel
          getOptionDouble(f.converted, r), //letTotConv
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

  def filterValidRows(rdd: RDD[Tml]): RDD[Tml] = {
    rdd
      .filter(r =>
        (r.ammissibilita.isEmpty && r.isValid.getOrElse("-").equalsIgnoreCase("SI") &&
          r.readType.getOrElse('-') == 'E') ||
          (r.ammissibilita.isDefined && Set('E', 'A').contains(r.readType.getOrElse('-'))))
  }
}

object TmlTable {
  def apply(inputPath: String): TmlTable = new TmlTable(inputPath)

  val flowFieldsOldRoute: FlowFields = FlowFields(
    service = "cod_servizio",
    pdr = "cod_pdr",
    date = "data_racc",
    pivaDistr = "piva_distr",
    pivaUtente = "piva_utente",
    measure = "let_tot_prel",
    converted = "let_tot_conv",
    serialNumberMis = "matr_mis",
    serialNumberConv = "matr_conv",
    local_file = "local_file",
    d_caricamento = "d_caricamento",
    ammissibilita = "ammissibilita"
  )
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy()
  val isValid = "val_dato"
  val readType = "tipo_lettura"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def formatDate: SimpleDateFormat = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  val schema: StructType = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(readType, isValid, newRouteField, partitioningColumn))

}
