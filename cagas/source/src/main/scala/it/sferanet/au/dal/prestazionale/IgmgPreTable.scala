package it.sferanet.au.dal.prestazionale

import it.sferanet.au.dal.prestazionale.IgmgPreTable._
import it.sferanet.au.model.prestazionale.IgmgPre
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col


class IgmgPreTable(inputPath: String) extends Serializable {

  def get(): RDD[IgmgPre] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        val f = IgmgPreTable.flowFields
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          List(IgmgPre(
            getString(f.service, r).toUpperCase + "PRE", //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionDate(f.date, IgmgPreTable.format, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionChar(IgmgPreTable.readType, r), //tipoLettura
            getOptionDouble(f.measure, r), //segnMis
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r), //matricola misuratore
            getOptionString(f.serialNumberConv, r), //matricola convertitore
            getOptionDouble(IgmgPreTable.coefCorr, r),
            getOptionInt(IgmgPreTable.cau_int_mis, r), //cau_int_mis
            getOptionInt(IgmgPreTable.cau_int_cor, r), //cau_int_cor
            getOptionString(f.local_file, r),
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[IgmgPre]()

      })
    // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)

  }

  def filterValidRows(rdd: RDD[IgmgPre]): RDD[IgmgPre] = {
    rdd
      .filter(r => r.cau_int_mis.isDefined || r.cau_int_cor.isDefined)
      .filter(r => Set('E', 'S').contains(r.readType.getOrElse('-')))
  }
}

object IgmgPreTable {

  def apply(inputPath: String): IgmgPreTable = new IgmgPreTable(inputPath)

  val flowFields: FlowFields = FlowFields(
    "cod_flusso",
    "cod_pdr",
    "data_misura",
    "piva_distr",
    "piva_utente",
    "let_misuratore_pre_int",
    "let_correttore_pre_int",
    "matr_mis_pre_int",
    "matr_conv_pre_int",
    "local_file",
    "d_caricamento",
    "ammissibilita"
  )
  val readType = "tipo_let" //tipo lettura
  val coefCorr = "coeff_corr_pre_int" //coefficiente correzione pre
  val cau_int_mis = "cau_int_mis"
  val cau_int_cor = "cau_int_cor"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  val schema = Flow.createSparkSchema(flowFields.getValues ::: List(readType, newRouteField, coefCorr, cau_int_mis, cau_int_cor, partitioningColumn))

  def format = Constants.STANDARD_FORMAT_DATE
}


