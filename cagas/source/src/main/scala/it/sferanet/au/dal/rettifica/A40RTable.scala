package it.sferanet.au.dal.rettifica

import it.sferanet.au.dal.rettifica.A40RTable._
import it.sferanet.au.model.rettifica.A40R
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class A40RTable(inputPath: String) extends Serializable {

  def get(): RDD[A40R] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          val f = flowFields
          List(A40R(
            getString(f.service, r).toUpperCase, //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionInt(motivazione, r),
            getOptionString(raccolta, r),
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
        else List[A40R]()
      })
      // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[A40R]): RDD[A40R] = {
    rdd
      .filter(r => Set(1, 2, 3, 4, 5).contains(r.motivation.getOrElse(-1)))
  }
}

object A40RTable {
  def apply(inputPath: String): A40RTable = new A40RTable(inputPath)

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
  val motivazione = "mot_ret_lett"
  val raccolta = "tipo_rettifica"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def formatDate = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento = Constants.FORMAT_DATE_LOAD

  val schema = Flow.createSparkSchema(flowFields.getValues :::
    List(motivazione, newRouteField, raccolta, partitioningColumn))
}

  
