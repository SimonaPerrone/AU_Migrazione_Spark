package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.FDDTable._
import it.sferanet.au.model.prestazionale.FDD
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class FDDTable(inputPath: String) extends Serializable {

  def get(): RDD[FDD] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = flowFields
        if (isNewRouteVersion) {
          List(FDD(
            getString(f.service, r).toUpperCase, //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionString(raccolta, r),
            getOptionChar(readType, r), //tipoLettura
            getOptionDate(f.date, formatDate, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionDouble(f.measure, r), //letTotPrel
            getOptionDouble(f.converted, r), //letTotConv
            getOptionString(f.serialNumberMis, r),
            getOptionString(f.serialNumberConv, r),
            getOptionString(f.local_file, r),
            getOptionDate(f.d_caricamento, formatDCaricamento, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[FDD]()
      }
      )
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[FDD]): RDD[FDD] = {
    rdd
      .filter(r => Set('E', 'A').contains(r.readType.getOrElse('-')))
  }
}

object FDDTable {
  def apply(inputPath: String): FDDTable = new FDDTable(inputPath)

  val flowFields: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_prest",
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
  val readType = "tipo_lettura"
  val raccolta = "raccolta"
  val dataStandardSost = "data_prest"
  val measureStandardSost = "let_tot_prel"
  val convertedStandardSost = "let_tot_conv"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def formatDate = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento = Constants.FORMAT_DATE_LOAD


  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(raccolta, readType, newRouteField, dataStandardSost, measureStandardSost, convertedStandardSost, partitioningColumn))

}




  
