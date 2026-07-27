package it.sferanet.au.dal.periodico

import it.sferanet.au.dal.periodico.TglTable._
import it.sferanet.au.model.periodico.Tgl
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import java.text.SimpleDateFormat

class TglTable(inputPath: String) extends Serializable {

  def get(): RDD[Tgl] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        Tgl(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, formatDate, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getOptionChar(readType, r), //tipoLettura
          getOptionString(isValid, r), //isValid
          getOptionDouble(f.measure, r), //letTotPrel
          getOptionDouble(f.converted, r), //letTotConv
          getSerialNumberMisMisureGiornaliere(r, flowFieldsNewRoute, flowFieldsOldRoute),
          getSerialNumberConvMisureGiornaliere(r, flowFieldsNewRoute, flowFieldsOldRoute),
          getOptionString(f.local_file, r),
          getOptionDate(f.d_caricamento, formatDCaricamento, r),
          getOptionString(f.ammissibilita, r),
          isNewRouteVersion
        )
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[Tgl]): RDD[Tgl] = {
    rdd
      .filter(r => Set('E').contains(r.readType.getOrElse('-')) &&
        ((r.ammissibilita.isEmpty && r.isValid.getOrElse("-").equalsIgnoreCase("SI")) ||
          r.ammissibilita.isDefined))
  }
}

object TglTable {
  def apply(inputPath: String): TglTable = new TglTable(inputPath)

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
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy(serialNumberMis = "matr_mis_giornaliere", serialNumberConv = "matr_conv_giornaliere")
  val readType = "tipo_lettura"
  val isValid = "val_dato_mens"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "mese_comp"

  def formatDate: SimpleDateFormat = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  val schema: StructType = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(readType, isValid, newRouteField, partitioningColumn))

}
