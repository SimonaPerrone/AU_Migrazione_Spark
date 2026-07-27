package it.sferanet.au.dal.rcu

import it.sferanet.au.model.{RcuGasVarConvertitore, RcuGasVarConvertitoreFields}
import it.sferanet.au.utilities.Environment
import it.sferanet.au.utilities.ParquetUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit

import java.text.SimpleDateFormat
import java.util.Date

class RcuGasVarConvertitoreTable(inputPath: String) extends Serializable {

  def get(): RDD[RcuGasVarConvertitore] = {
    val defaultLowerBound: Date = RcuGasVarConvertitoreTable.format.parse("1900-01-01")
    val defaultUpperBound: Date = RcuGasVarConvertitoreTable.format.parse("3000-01-01")

    Environment.getSqlContext.read
      .parquet(inputPath)
      .withColumn("t_pre_conv", lit("SI"))
      .rdd
      .map(r => RcuGasVarConvertitore(
        getOptionDate("d_data_inizio", RcuGasVarConvertitoreTable.format, r).getOrElse(defaultLowerBound),
        getOptionDate("d_data_fine", RcuGasVarConvertitoreTable.format, r).getOrElse(defaultUpperBound),
        getString("n_id_pdr", r),
        getString("t_pre_conv", r),
        getOptionInt("n_num_cifre_convertitore", r)
      ))
  }
}

object RcuGasVarConvertitoreTable {
  val flowFields: RcuGasVarConvertitoreFields = RcuGasVarConvertitoreFields(
    "d_data_inizio",
    "d_data_fine",
    "t_codice_pdr",
    "t_pre_conv",
    "n_num_cifre_convertitore"
  )

  def format = new SimpleDateFormat("yyyy-MM-dd")
}