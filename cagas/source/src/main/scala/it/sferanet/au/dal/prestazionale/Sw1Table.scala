package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.Sw1Table._
import it.sferanet.au.model.prestazionale.Sw1
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class Sw1Table(inputPath: String) extends Serializable {

  def get(): RDD[Sw1] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        val isNewRouteVersion = Flow.getIsNewRouteVersion(r, newRouteField)
        val cod_flow = getString(codFlusso, r)
        val f = flowFields
        // SW1 (vecchio tracciato) ha ammissibilita' sempre NULL e cod_flow = '0350' per righe valide
        if (!isNewRouteVersion && isValid(cod_flow)) {
          List(Sw1(
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
            isNewRoute = false
          ))
        }
        else List[Sw1]()
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[Sw1]): RDD[Sw1] = {
    rdd
      .filter(r => r.ammissibilita.isEmpty)
      .filter(r => Set('E').contains(r.readType.getOrElse('-')))
  }
}

object Sw1Table {

  def isValid(codeFlow: String): Boolean =
    codeFlow == "0350"

  def apply(inputPath: String): Sw1Table = new Sw1Table(inputPath)

  val flowFields: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_deco_switch",
    "piva_distr",
    "piva_utente",
    "segn_mis_sost",
    "segn_conv",
    "matr_mis",
    "matr_conv",
    "local_file",
    "d_caricamento",
    "ammissibilita"
  )
  val readType = "tipo_lettura"
  val newRouteField = "ammissibilita"
  val segnanteSost = "segn_mis_sost"
  val segnanteSostConv = "segn_conv"
  val partitioningColumn = "annomese"
  val codFlusso = "cod_flusso"

  val schema = Flow.createSparkSchema(flowFields.getValues :::
    List(codFlusso, segnanteSost, newRouteField, readType, segnanteSostConv, partitioningColumn))

  def formatDate = Constants.STANDARD_FORMAT_DATE

  def formatDCaricamento = Constants.FORMAT_DATE_LOAD

}
