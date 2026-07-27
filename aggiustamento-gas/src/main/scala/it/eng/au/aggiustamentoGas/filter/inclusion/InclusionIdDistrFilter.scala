package it.eng.au.aggiustamentoGas.filter.inclusion

import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.schema.agg.InclusionFilterSchema
import it.eng.au.aggiustamentoGas.schema.rcugas.RcuGasConnessioniDistr2Schema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

class InclusionIdDistrFilter(private val rcuGasConnessioniDistr2DF: DataFrame) extends InclusionFilterController {

  /**
   * @return true if the filter is enabled in params.properties
   */
  def isEnabled: Boolean = {
    Environment.isIdDistrInclusionFilterEnabled.equalsIgnoreCase("true")
  }

  def filter(measures: RDD[Flow]): RDD[Flow] = {
    if (isEnabled) {
      val distrToInclude: DataFrame = inclusionFileDf
        .where(isNotNullNorEmpty(col(InclusionFilterSchema.id_distr)))
        .where(isNullOrEmpty(col(InclusionFilterSchema.pdr)))
        .where(isNullOrEmpty(col(InclusionFilterSchema.piva_udd)))
        .select(col(InclusionFilterSchema.id_distr))
        .distinct

      //GET data at sqoop date, BCjoin with input csvDF, Back2RDD to filter measures
      val distrAtSqoopDateDF = rcuGasConnessioniDistr2DF
        .withColumn(RcuGasConnessioniDistr2Schema.d_data_inizio_conn, greatest(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), col(RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione)))
        .withColumn(RcuGasConnessioniDistr2Schema.d_data_fine_conn, least(col(RcuGasConnessioniDistr2Schema.d_data_fine_conn), col(RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione)))
        .where(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), lit("1970-01-01")).cast(DateType) <= sqoopDateExpr)
        .where(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_fine_conn), lit("2900-01-01")).cast(DateType) >= sqoopDateExpr)
        //TODO è necessario inserire anche qui d_data_inizio_aggregazione e d_data_fine_aggregazione?
        .select(RcuGasConnessioniDistr2Schema.n_id_distr, RcuGasConnessioniDistr2Schema.t_codice_pdr)
      //query using dataframes to optimize joins (hoping into a broadcast join)
      val pdrToIncludeByInputDistr = distrAtSqoopDateDF
        .join(broadcast(distrToInclude), distrToInclude.col(InclusionFilterSchema.id_distr) === distrAtSqoopDateDF.col(RcuGasConnessioniDistr2Schema.n_id_distr) ,"inner")
        .select(RcuGasConnessioniDistr2Schema.t_codice_pdr)
        .distinct
        .rdd
        .map(row => (row.getAs[String](RcuGasConnessioniDistr2Schema.t_codice_pdr), true))

      measures.keyBy(_.pdr)
        .join(pdrToIncludeByInputDistr) //select only those pdrs we are interested in
        .map({ case (pdr, (measure, flag)) => measure})
    } else {
      measures
    }
  }

  override def toString: String = "Inclusion Filter By Id Distr"

}
