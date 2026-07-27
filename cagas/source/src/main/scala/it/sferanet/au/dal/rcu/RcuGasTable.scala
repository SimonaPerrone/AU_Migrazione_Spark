package it.sferanet.au.dal.rcu

import it.sferanet.au.model.{RcuGasFields, RcuGasMassivo}
import it.sferanet.au.schema.RcuGasMassivoPSchema
import it.sferanet.au.utilities.Environment
import it.sferanet.au.utilities.ParquetUtils.getString
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col

import java.text.SimpleDateFormat
import java.util.Date

class RcuGasTable(inputPath: String, partition: String, fields: RcuGasFields) extends Serializable {

  def this(inputPath: String, partition: String) = {
    this(inputPath, partition, RcuGasTable.flowFields)
  }

  def get(): RDD[RcuGasMassivo] = {
    val f = fields
    val rcuGasTable =
      if (inputPath.contains("freeze")) {
        Environment.getSqlContext.read.parquet(inputPath)
          .where(col("execution_id") === partition)
          .withColumn(RcuGasMassivoPSchema.d_data_inizio_for, col(RcuGasMassivoPSchema.d_data_inizio_for).cast("string"))
          .withColumn(RcuGasMassivoPSchema.data_fine_for, col(RcuGasMassivoPSchema.data_fine_for).cast("string"))
      } else {
        Environment.getSqlContext.read.parquet(inputPath)
      }
    rcuGasTable
      .rdd
      .map(r => RcuGasMassivo(
        if (!r.schema.exists(v => v.name == f.mindate) || r.getAs(f.mindate) == null) new Date(0) else RcuGasTable.format.parse(r.getAs(f.mindate)), //dataInizio
        if (!r.schema.exists(v => v.name == f.maxdate) || r.getAs(f.maxdate) == null) new Date(Long.MaxValue) else RcuGasTable.format.parse(r.getAs(f.maxdate)), //dataFin
        r.getAs(f.t_codice_pdr).toString, //codice_pdr
        if (r.getAs(f.t_cod_cat_uso) == null) "" else r.getAs(f.t_cod_cat_uso), //categoria d'uso
        if (r.getAs(f.t_cod_profilo) == null) "" else r.getAs(f.t_cod_profilo), //prof
        if (r.getAs("t_processo") == null) "" else r.getAs("t_processo").toString,
        if (r.getAs("id_regione_climatica") == null) 0 else r.getAs("id_regione_climatica").toString.toInt,
        if (r.getAs("t_comune_istat_pdr") == null) {
          if (r.getAs("t_comune_istatforn") != null) r.getAs("t_comune_istatforn").toString else ""
        } else r.getAs("t_comune_istat_pdr").toString,
        if (r.getAs("t_anno_termico") == null) 0 else r.getAs("t_anno_termico").toString.toInt,
        if (r.getAs("n_prelievo_annuo") == null) "" else r.getAs("n_prelievo_annuo").toString,
        getString("n_id_pdr", r)
      )
      )
  }
}

object RcuGasTable {
  val flowFields: RcuGasFields = RcuGasFields(
    "d_data_inizio_for",
    "data_fine_for",
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