package it.sferanet.au.dal.autolettura

import it.sferanet.au.dal.autolettura.TavTable._
import it.sferanet.au.model.autolettura.Tav
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import java.text.SimpleDateFormat

class TavTable(inputPath: String) extends Serializable {

  def get(): RDD[Tav] = {
    Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .map(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = if (isNewRouteVersion) flowFieldsNewRoute else flowFieldsOldRoute
        Tav(
          getString(f.service, r).toUpperCase, //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, formatDate, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getEsito(outcome, r), //esito
          getOptionDouble(f.measure, r), //letTotPrel
          getOptionDouble(f.converted, r), //letTotConv
          getOptionString(f.serialNumberMis, r),
          getOptionString(f.serialNumberConv, r),
          getOptionString(f.local_file, r),
          getOptionDate(f.d_caricamento, formatDCaricamento, r),
          getOptionString(f.ammissibilita, r),
          isNewRouteVersion
        )
      }
      )
  }
}

object TavTable {
  def apply(inputPath: String): TavTable = new TavTable(inputPath)

  val flowFieldsOldRoute: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_com_autolet_cf",
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
  val flowFieldsNewRoute: FlowFields = flowFieldsOldRoute.copy(date = "data_racc")
  val outcome = "esito_val"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def formatDate: SimpleDateFormat = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento: SimpleDateFormat = Constants.FORMAT_DATE_LOAD

  val schema: StructType = Flow.createSparkSchema(flowFieldsOldRoute.getValues ::: flowFieldsNewRoute.getValues ::: List(outcome, newRouteField, partitioningColumn))

}
