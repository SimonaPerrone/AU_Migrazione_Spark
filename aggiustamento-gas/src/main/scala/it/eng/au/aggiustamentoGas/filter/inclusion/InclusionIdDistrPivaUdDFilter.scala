package it.eng.au.aggiustamentoGas.filter.inclusion

import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.schema.agg.InclusionFilterSchema
import it.eng.au.aggiustamentoGas.schema.rcugas.{RcuGasConnessioniDistr2Schema, RcuGasMassivoPSchema}
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

class InclusionIdDistrPivaUdDFilter(private val rcuGasMassivoPDF: DataFrame,
                                    private val rcuGasConnessioniDistr2DF: DataFrame) extends InclusionFilterController {

  /**
   * @return true if the filter is enabled in params.properties
   */
  def isEnabled: Boolean = {
    Environment.isIdDistrPivaUddInclusionFilterEnabled.equalsIgnoreCase("true")
  }

  def filter(measures: RDD[Flow]): RDD[Flow] = {
    if (isEnabled) {
      val distrUddToInclude: DataFrame = inclusionFileDf
        .where(isNotNullNorEmpty(col(InclusionFilterSchema.id_distr)))
        .where(isNotNullNorEmpty(col(InclusionFilterSchema.piva_udd)))
        .where(isNullOrEmpty(col(InclusionFilterSchema.pdr)))
        .select(col(InclusionFilterSchema.id_distr), col(InclusionFilterSchema.piva_udd))
        .distinct

      //GET data at sqoop date, BCjoin with input csvDF, Back2RDD to filter measures
      //query using dataframes to optimize joins (hoping into a broadcast join)
      val distrAtSqoopDateDF = rcuGasConnessioniDistr2DF
        .withColumn(RcuGasConnessioniDistr2Schema.d_data_inizio_conn, greatest(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), col(RcuGasConnessioniDistr2Schema.d_data_inizio_aggregazione)))
        .withColumn(RcuGasConnessioniDistr2Schema.d_data_fine_conn, least(col(RcuGasConnessioniDistr2Schema.d_data_fine_conn), col(RcuGasConnessioniDistr2Schema.d_data_fine_aggregazione)))
        .where(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_inizio_conn), lit("1970-01-01")).cast(DateType) <= sqoopDateExpr)
        .where(coalesce(col(RcuGasConnessioniDistr2Schema.d_data_fine_conn), lit("2900-01-01")).cast(DateType) >= sqoopDateExpr)
        //TODO, anche qui è necessario inserire d_data_inizio_aggregazione e d_data_fine_aggregazione?
        .select(RcuGasConnessioniDistr2Schema.n_id_distr, RcuGasConnessioniDistr2Schema.t_codice_pdr)
      val massivoAtSqoopDateDF = rcuGasMassivoPDF
        .where(coalesce(col(RcuGasMassivoPSchema.d_data_inizio_for), lit("1970-01-01")).cast(DateType) <= sqoopDateExpr)
        .where(coalesce(col(RcuGasMassivoPSchema.data_fine_for), lit("2900-01-01")).cast(DateType) >= sqoopDateExpr)
        .select(RcuGasMassivoPSchema.t_codice_pdr, RcuGasMassivoPSchema.piva_udd)
      val joinedRcuGas =  distrAtSqoopDateDF.join(massivoAtSqoopDateDF, massivoAtSqoopDateDF.col(RcuGasMassivoPSchema.t_codice_pdr) === distrAtSqoopDateDF.col(RcuGasConnessioniDistr2Schema.t_codice_pdr), "inner")
        .drop(distrAtSqoopDateDF.col(RcuGasConnessioniDistr2Schema.t_codice_pdr))

      val bcJoinExpression = distrUddToInclude.col(InclusionFilterSchema.piva_udd) === joinedRcuGas.col(RcuGasMassivoPSchema.piva_udd) and
        distrUddToInclude.col(InclusionFilterSchema.id_distr) === joinedRcuGas.col(RcuGasConnessioniDistr2Schema.n_id_distr)
      val pdrsToInclude = joinedRcuGas
        .join(broadcast(distrUddToInclude), bcJoinExpression,"inner")
        .select(RcuGasMassivoPSchema.t_codice_pdr)
        .rdd
        .map(row => (row.getAs[String](RcuGasMassivoPSchema.t_codice_pdr), true))

      measures.keyBy(_.pdr)
        .join(pdrsToInclude)//select only those pdrs we are interested in
        .map({ case (pdr, (measure, flag)) => measure})
    } else {
      measures
    }
  }

  override def toString: String = "Inclusion Filter By Id Distr and piva UdD"
}
