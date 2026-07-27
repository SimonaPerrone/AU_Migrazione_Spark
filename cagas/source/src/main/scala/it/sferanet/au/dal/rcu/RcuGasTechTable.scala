package it.sferanet.au.dal.rcu

import it.sferanet.au.model.{RcuGasFields, RcuGasMassivoTech}
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

import java.text.SimpleDateFormat
import java.util.Date

class RcuGasTechTable(inputPath: String, partition: String, fields: RcuGasFields) extends Serializable {

  def this(inputPath: String, partition: String) = {
    this(inputPath, partition, RcuGasTable.flowFields)
  }

  def get(): RDD[RcuGasMassivoTech] = {
    val f = fields
    val rcuTech = if (inputPath.contains("freeze")) {
      Environment.getSqlContext.read.parquet(inputPath)
        .where(col("execution_id") === partition)
        .withColumn("data_inizio_tech", col("data_inizio_tech").cast("string"))
        .withColumn("data_fine_tech", col("data_fine_tech").cast("string"))
    } else {
      Environment.getSqlContext.read.parquet(inputPath)
    }

    rcuTech
      .rdd
      .map(r => RcuGasMassivoTech(
        if (!r.schema.exists(v => v.name == f.mindate) || r.getAs(f.mindate) == null) new Date(0) else RcuGasTable.format.parse(r.getAs(f.mindate)), //dataInizio
        if (!r.schema.exists(v => v.name == f.maxdate) || r.getAs(f.maxdate) == null) new Date(Long.MaxValue) else RcuGasTable.format.parse(r.getAs(f.maxdate)), //dataFin
        r.getAs(f.t_codice_pdr).toString, //codice_pdr
        if (r.getAs(f.n_coeff_correzione) == null) None else Some(r.getAs(f.n_coeff_correzione).toString.toDouble), //coeff_corr
        if (r.getAs(f.t_misuratore_integrato) != null) Some(r.getAs(f.t_misuratore_integrato).toString.trim) else None, //mis_int
        if (r.getAs(f.t_pre_conv) != null) Some(r.getAs(f.t_pre_conv).toString.trim) else None, //pre_conv
        if (r.getAs(f.n_num_cifre_misuratore) == null) None else Some(r.getAs(f.n_num_cifre_misuratore).toString.toDouble.toInt), //cifre_mis
        if (r.getAs(f.n_num_cifre_convertitore) == null) None else Some(r.getAs(f.n_num_cifre_convertitore).toString.toDouble.toInt) //cifre_conv

      ))
  }
}

object RcuGasTechTable {
  val flowFields: RcuGasFields = RcuGasFields(
    "data_inizio_tech",
    "data_fine_tech",
    "t_codice_pdr",
    "t_cod_cat_uso",
    "t_cod_profilo",
    "n_coeff_correzione",
    "t_misuratore_integrato",
    "t_pre_conv",
    "n_num_cifre_misuratore",
    "n_num_cifre_convertitore"
  )

  def format = new SimpleDateFormat("yyyy-MM-dd")
}