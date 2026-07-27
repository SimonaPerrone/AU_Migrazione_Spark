package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.IgmrPostTable._
import it.sferanet.au.model.prestazionale.IgmrPost
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class IgmrPostTable(inputPath: String) extends Serializable {

  def get(): RDD[IgmrPost] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        val f = IgmrPostTable.flowFields
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          List(IgmrPost(
            getString(f.service, r).toUpperCase + "POST", //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionDate(f.date, IgmrPostTable.format, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionDouble(f.measure, r), //segnMis
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r), //matricola misuratore
            getOptionString(f.serialNumberConv, r), //matricola convertitore
            getOptionInt(IgmrPostTable.mot_ret_lett, r),
            getOptionDouble(IgmrPostTable.coefCorr, r),
            getOptionInt(IgmrPostTable.cau_int_mis, r), //cau_int_mis
            getOptionInt(IgmrPostTable.cau_int_cor, r), //cau_int_cor
            getOptionString(f.local_file, r),
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[IgmrPost]()
      })
    rdd
  }
}

object IgmrPostTable {

  def apply(inputPath: String): IgmrPostTable = new IgmrPostTable(inputPath)

  val flowFields: FlowFields = FlowFields(
    "cod_flusso",
    "cod_pdr",
    "data_misura",
    "piva_distr",
    "piva_utente",
    "let_misuratore_post_int",
    "let_correttore_post_int",
    "matr_mis_post_int",
    "matr_conv_post_int",
    "local_file",
    "d_caricamento",
    "ammissibilita"
  )
  val coefCorr = "coeff_corr_post_int" //coefficiente correzione post
  val cau_int_mis = "cau_int_mis"
  val cau_int_cor = "cau_int_cor"
  val mot_ret_lett = "mot_ret_lett"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(newRouteField, coefCorr, cau_int_mis, cau_int_cor, mot_ret_lett, partitioningColumn))

  def format = Constants.STANDARD_FORMAT_DATE
}


