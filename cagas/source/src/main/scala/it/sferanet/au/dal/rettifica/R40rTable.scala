package it.sferanet.au.dal.rettifica

import it.sferanet.au.dal.rettifica.R40rTable._
import it.sferanet.au.model.rettifica.R40r
import it.sferanet.au.model.{Flow, FlowFields}
import it.sferanet.au.utilities.ParquetUtils._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

class R40rTable(inputPath: String) extends Serializable {

  def get(): RDD[R40r] = {
    val rdd = Environment.getSqlContext.read
      .schema(schema)
      .parquet(inputPath)
      .filter(Flow.flowFilter(partitioningColumn, col(partitioningColumn), col(newRouteField)))
      .rdd
      .flatMap(r => {
        if (Flow.getIsNewRouteVersion(r, newRouteField)) {
          val f = flowFields
          List(R40r(
            getString(f.service, r).toUpperCase, //flusso
            getString(f.pdr, r), //cod_pdr
            getOptionString(raccolta, r), //tipoLettura
            getOptionDate(f.date, format, r), //date
            getOptionString(f.pivaDistr, r), //pivaDistr
            getOptionString(f.pivaUtente, r), //pivaUtente
            getOptionDouble(f.measure, r), //segnMis
            getOptionDouble(f.converted, r), //segnConv
            getOptionString(f.serialNumberMis, r), //matricola misuratore
            getOptionString(f.serialNumberConv, r), //matricola convertitore
            getOptionInt(R40rTable.motRettLett, r),
            getOptionString(f.local_file, r), //local file
            getOptionDate(f.d_caricamento, Constants.FORMAT_DATE_LOAD, r),
            getOptionString(f.ammissibilita, r),
            isNewRoute = true
          ))
        }
        else List[R40r]()
      })
      // AU-739: aggiunge filtro lettura
    filterValidRows(rdd)
  }

  def filterValidRows(rdd: RDD[R40r]): RDD[R40r] = {
    rdd
      .filter(r => Set(1, 2, 3, 4, 5).contains(r.motivation.getOrElse(-1)))
  }
}


object R40rTable {

  def apply(inputPath: String): R40rTable = new R40rTable(inputPath)

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
  val raccolta = "tipo_rettifica"
  val motRettLett = "mot_ret_lett"
  val newRouteField = "ammissibilita"
  val partitioningColumn = "annomese"

  def format = Constants.STANDARD_FORMAT_DATE

  val schema = Flow.createSparkSchema(flowFields.getValues :::
    List(raccolta, newRouteField, motRettLett, partitioningColumn))
}
