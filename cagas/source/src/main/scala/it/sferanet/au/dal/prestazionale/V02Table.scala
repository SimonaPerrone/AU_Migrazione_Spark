package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.V02Table._
import it.sferanet.au.model.prestazionale.V02
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class V02Table(inputPath: String) extends Serializable {

  def get(): RDD[V02] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        if (isNewRouteVersion) {
          val f = flowFields
          List(V02(
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
            getOptionString(raccolta, r), //raccolta
            getOptionString(f.local_file, r), //local file
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[V02]()
      }
      )
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[V02]): RDD[V02] = {
    rdd
      .filter(r => Set('E', 'A', 'S').contains(r.readType.getOrElse('-')))
  }
}

object V02Table {

  def apply(inputPath: String): V02Table = new V02Table(inputPath)

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
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def format = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFields.getValues :::
    List(readType, newRouteField, raccolta, partitioningColumn))

}