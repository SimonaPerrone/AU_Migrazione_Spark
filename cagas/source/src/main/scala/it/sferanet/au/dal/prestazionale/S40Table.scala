package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.S40Table._
import it.sferanet.au.model.prestazionale.S40
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class S40Table(inputPath: String) extends Serializable {

  def get(): RDD[S40] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          val f = flowFields
          List(S40(
            getString(f.service, r).toUpperCase, //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionChar(readType, r), //tipoLettura
            getOptionDate(f.date, format, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionDouble(f.measure, r), //segnMis
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r), //matricola misuratore
            getOptionString(f.serialNumberConv, r), //matricola convertitore
            getOptionString(collected, r),
            getOptionString(f.local_file, r), //local file
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        } else List[S40]()
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[S40]): RDD[S40] = {
    rdd
      .filter(r => Set('E', 'A', 'S').contains(r.readType.getOrElse('-')))
  }
}

object S40Table {

  def apply(inputPath: String): S40Table = new S40Table(inputPath)

  val flowFields: FlowFields = FlowFields(
    "cod_flusso",
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
  val collected = "raccolta"
  val readType = "tipo_lettura"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(collected, newRouteField, readType, partitioningColumn))

  def format = Constants.STANDARD_FORMAT_DATE

}



