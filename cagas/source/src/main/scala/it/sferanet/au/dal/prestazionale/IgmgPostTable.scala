package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.IgmgPostTable._
import it.sferanet.au.model.prestazionale.IgmgPost
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class IgmgPostTable(inputPath: String) extends Serializable {

  def get(): RDD[IgmgPost] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        val f = IgmgPostTable.flowFields
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          List(IgmgPost(
            getString(f.service, r).toUpperCase + "POST", //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionDate(f.date, IgmgPostTable.format, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionChar(IgmgPostTable.readType, r), //tipoLettura
            getOptionDouble(f.measure, r), //segnMis
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r), //matricola misuratore
            getOptionString(f.serialNumberConv, r), //matricola convertitore
            getOptionDouble(IgmgPostTable.coefCorr, r),
            getOptionInt(IgmgPostTable.cau_int_mis, r), //cau_int_mis
            getOptionInt(IgmgPostTable.cau_int_cor, r), //cau_int_cor
            getOptionString(f.local_file, r),
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[IgmgPost]()
      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[IgmgPost]): RDD[IgmgPost] = {
    rdd
      .filter(r => r.cau_int_mis.isDefined || r.cau_int_cor.isDefined)
      .filter(r => Set('E', 'S').contains(r.readType.getOrElse('-')))
  }
}

object IgmgPostTable {

  def apply(inputPath: String): IgmgPostTable = new IgmgPostTable(inputPath)

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
  val readType = "tipo_let" //tipo lettura
  val coefCorr = "coeff_corr_post_int" //coefficiente correzione post
  val cau_int_mis = "cau_int_mis"
  val cau_int_cor = "cau_int_cor"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(readType, newRouteField, coefCorr, cau_int_mis, cau_int_cor, partitioningColumn))

  def format = Constants.STANDARD_FORMAT_DATE
}


