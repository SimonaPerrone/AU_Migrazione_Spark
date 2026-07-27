package it.sferanet.au.dal.rcu

import it.sferanet.au.model.RcuGasProfilo
import it.sferanet.au.utilities.Environment
import it.sferanet.au.utilities.ParquetUtils._
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat

class RcuGasProfiloTable(inputPath: String) extends Serializable {

  def get(): RDD[RcuGasProfilo] = {
    Environment.getSqlContext.read
      .parquet(inputPath)
      .rdd
      .map(r => RcuGasProfilo(
        getString("n_id_var_profilo", r),
        getString("n_id_pdr", r),
        getInt("t_anno", r),
        getString("t_cod_profilo", r),
        getString("t_cod_cat_uso", r),
        getString("t_cod_classe_prelievo", r),
        getOptionDate("d_data_inizio", RcuGasProfiloTable.format, r),
        getOptionDate("d_data_fine", RcuGasProfiloTable.format, r)
      ))
  }
}

object RcuGasProfiloTable {
  def format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
}
