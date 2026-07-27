package it.eng.au.aggiustamentoGas.filter.inclusion

import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.schema.agg.InclusionFilterSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

class InclusionPdrFilter() extends InclusionFilterController {

  /**
   * @return true if the filter is enabled in params.properties
   */
  def isEnabled: Boolean = {
    Environment.isPdrInclusionFilterEnabled.equalsIgnoreCase("true")
  }

  /**
   * Ingest only some pdrs specified as input
   * @param measures an rdd of Flow
   * @return an rdd of Flow containing only Flow with pdr specified in the file at ${filter.inclusion.file.path}
   */
  def filter(measures: RDD[Flow]): RDD[Flow] = {
    if (isEnabled) {
      val pdrToInclude: RDD[(String, Boolean)] = inclusionFileDf
        .where(isNotNullNorEmpty(col(InclusionFilterSchema.pdr)))
        .where(isNullOrEmpty(col(InclusionFilterSchema.id_distr)))
        .where(isNullOrEmpty(col(InclusionFilterSchema.piva_udd)))
        .select(col(InclusionFilterSchema.pdr))
        .distinct
        .rdd
        .map(row => (row.getAs[String](InclusionFilterSchema.pdr), true))

      measures.keyBy(_.pdr)
        .join(pdrToInclude)
        .map({ case (pdr, (measures, flag)) => measures })
    } else {
      measures
    }
  }
  override def toString: String = "Inclusion Filter By Pdr"
}
