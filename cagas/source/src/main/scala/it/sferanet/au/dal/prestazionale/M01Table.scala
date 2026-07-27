package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.M01Table._
import it.sferanet.au.model.prestazionale.M01
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class M01Table(inputPath: String) extends Serializable {

  def get(): RDD[M01] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, M01Table.newRouteField)
        if (isNewRouteVersion) { // del flusso M01 viene considerato soltanto il tracciato Standard
          val f = flowFields
          List(M01(
            getString(f.service, r).toUpperCase, //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionChar(M01Table.readType, r), //tipoLettura
            getOptionDate(f.date, M01Table.format, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionDouble(f.measure, r), //segnMis
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r), //matricola misuratore
            getOptionString(f.serialNumberConv, r), //matricola convertitore
            getOptionString(M01Table.collected, r), //raccolta
            getOptionString(f.local_file, r), //local file
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[M01]()
      }
      )
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[M01]): RDD[M01] = {
    rdd
      .filter(r => Set('E', 'A', 'S').contains(r.readType.getOrElse('-')))
  }
}

object M01Table {

  def apply(inputPath: String): M01Table = new M01Table(inputPath)

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
  val collected = "raccolta"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def format = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(readType, newRouteField, collected, partitioningColumn))

}


