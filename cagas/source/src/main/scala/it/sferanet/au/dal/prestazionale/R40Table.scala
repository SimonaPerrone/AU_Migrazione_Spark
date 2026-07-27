package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.R40Table._
import it.sferanet.au.model.prestazionale.R40
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class R40Table(inputPath: String) extends Serializable {

  def get(): RDD[R40] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          val f = R40Table.flowFields
          List(R40(
            getString(f.service, r).toUpperCase, //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionChar(R40Table.readType, r), //tipoLettura
            getOptionDate(f.date, M01Table.format, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionDouble(f.measure, r), //segnMis
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r), //matricola misuratore
            getOptionString(f.serialNumberConv, r), //matricola convertitore
            getOptionString(R40Table.collected, r),
            getOptionString(f.local_file, r), //local file
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[R40]()
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[R40]): RDD[R40] = {
    rdd
      .filter(r => Set('E', 'A', 'S').contains(r.readType.getOrElse('-')))
  }
}

object R40Table {

  def apply(inputPath: String): R40Table = new R40Table(inputPath)

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
  val readType = "tipo_lettura"
  val newRouteField = "ammissibilita"
  val collected = "raccolta"
  val partitioningColumn = "annomese"

  def format = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(readType, newRouteField, collected, partitioningColumn))

}
