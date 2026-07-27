package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.Swg1Table._
import it.sferanet.au.model.prestazionale.Swg1
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class Swg1Table(inputPath: String) extends Serializable {

  def get(): RDD[Swg1] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        // SWG1 (nuovo tracciato dopo 202112) ha sempre ammissibilita' valorizzata
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val f = flowFields
        if (isNewRouteVersion) {
          List(Swg1(
            getString(f.service, r).toUpperCase, //flusso
            getString(f.pdr, r), //cod_pdr
            Constants.getDate(formatDate, r.getAs(f.date)), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionChar(readType, r), //tipoLettura
            getOptionDouble(f.measure, r), //segnMisSost
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r),
            getOptionString(f.serialNumberConv, r),
            getOptionString(f.local_file, r),
            getOptionDate(f.d_caricamento, formatDCaricamento, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[Swg1]()
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[Swg1]): RDD[Swg1] = {
    rdd
      .filter(r => Set('E', 'A').contains(r.readType.getOrElse('-')))
  }
}

object Swg1Table {

  def apply(inputPath: String): Swg1Table = new Swg1Table(inputPath)

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
  val newRouteField = "ammissibilita"
  val dataSost = "data_prest"
  val segnanteSost = "let_tot_prel"
  val segnanteSostConv = "let_tot_conv"
  val partitioningColumn = "annomese"

  def formatDate = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento = Constants.FORMAT_DATE_LOAD

  val schema = Flow.createSparkSchema(flowFields.getValues :::
    List(segnanteSost, newRouteField, readType, dataSost, segnanteSostConv, partitioningColumn))
}


