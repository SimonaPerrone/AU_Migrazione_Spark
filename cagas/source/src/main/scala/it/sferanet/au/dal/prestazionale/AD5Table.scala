package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.AD5Table._
import it.sferanet.au.model.prestazionale.AD5
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class AD5Table(inputPath: String) extends Serializable {

  def get(): RDD[AD5] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          val f = flowFields
          List(AD5(
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
        else List[AD5]()
      })
      // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[AD5]): RDD[AD5] = {
    rdd
      .filter(r => Set('E', 'A', 'S').contains(r.readType.getOrElse('-')))
  }
}

object AD5Table {
  def apply(inputPath: String): AD5Table = new AD5Table(inputPath)

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

  def formatDate = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento = Constants.FORMAT_DATE_LOAD

  val readType = "tipo_lettura"
  val raccolta = "raccolta"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(raccolta, readType, newRouteField, partitioningColumn))

}







  









  
