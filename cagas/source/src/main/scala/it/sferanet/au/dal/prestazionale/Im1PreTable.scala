package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.Im1PreTable._
import it.sferanet.au.model.prestazionale.Im1Pre
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class Im1PreTable(inputPath: String) extends Serializable {

  def get(): RDD[Im1Pre] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), null))
      .rdd
      .map(r => {
        val f = Im1PreTable.flowFields
        Im1Pre(
          getString(f.service, r).toUpperCase + "PRE", //flusso
          getString(f.pdr, r), //cod_pdr
          getOptionDate(f.date, format, r), //date
          getOptionString(f.pivaDistr, r), //pivaDistr
          getOptionString(f.pivaUtente, r), //pivaUtente
          getOptionChar(Im1PreTable.readType, r), //tipo_lettura
          getOptionDouble(f.measure, r), //segnMis
          getOptionDouble(f.converted, r), //segnConv
          Option(r.getAs[String](f.serialNumberMis)), //matricola misuratore
          Option(r.getAs[String](f.serialNumberConv)), //matricola convertitore
          getOptionDouble(Im1PreTable.coefCorr, r), //coefCorr
          getOptionInt(Im1PreTable.cau_int_mis, r), //cau_int_mis
          getOptionInt(Im1PreTable.cau_int_cor, r), //cau_int_cor
          getOptionString(f.local_file, r),
          getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
          getOptionString(f.ammissibilita, r),
          isNewRoute = false //Im1 solo vecchio flusso
        )
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[Im1Pre]): RDD[Im1Pre] = {
    rdd
      .filter(r => Set('E', 'S').contains(r.readType.getOrElse('-')))
  }
}

object Im1PreTable {
  def apply(inputPath: String): Im1PreTable = new Im1PreTable(inputPath)

  val flowFields: FlowFields = FlowFields(
    "cod_servizio",
    "cod_pdr",
    "data_esec_int",
    "piva_distr",
    "piva_utente",
    "PRE_let_misuratore",
    "PRE_let_correttore",
    "PRE_matr_mis",
    "PRE_matr_conv",
    "local_file",
    "d_caricamento",
    "ammissibilita"
  )
  val readType = "PRE_tipo_mis"
  val coefCorr = "PRE_coeff_corr"
  val cau_int_mis = "cau_int_mis"
  val cau_int_cor = "cau_int_cor"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(readType, coefCorr, cau_int_mis, cau_int_cor, partitioningColumn))

  def format = Constants.STANDARD_FORMAT_DATE
}
