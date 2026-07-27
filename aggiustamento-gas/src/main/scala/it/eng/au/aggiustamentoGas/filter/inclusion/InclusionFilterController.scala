package it.eng.au.aggiustamentoGas.filter.inclusion

import it.eng.au.aggiustamentoGas.model.measure.Flow
import it.eng.au.aggiustamentoGas.schema.agg.InclusionFilterSchema
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Utilizzato per forzare l'inclusione delle misure fornite come input nel file definito da [[Environment.getInclusionFilterPath]]
 */
trait InclusionFilterController {
  //NOTE: it is not collected and sent in broadcast since AU is not able to give us an estimate on the input. To avoid
  // failures on large input files we prefer to avoid broadcast.
  lazy val inclusionFileDf: DataFrame = Environment.getSpark.sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .schema(InclusionFilterSchema.createSparkSchema())
    .load(Environment.getInclusionFilterPath)
    .select(
      trim(col(InclusionFilterSchema.pdr)).as(InclusionFilterSchema.pdr),
      trim(col(InclusionFilterSchema.id_distr)).as(InclusionFilterSchema.id_distr),
      trim(col(InclusionFilterSchema.piva_udd)).as(InclusionFilterSchema.piva_udd)
    )
    .distinct
    .cache

  protected val sqoopDateExpr: Column =  from_unixtime(unix_timestamp(lit(Environment.getRcugasSqoopDate),"yyyyMMdd"),"yyyy-MM-dd").cast(DateType)

  def filter(measures: RDD[Flow]): RDD[Flow]

  def isEnabled: Boolean

  protected def isNotNullNorEmpty(c: Column): Column = not(isNullOrEmpty(c))

  protected def isNullOrEmpty(c: Column): Column = c.isNull or (trim(c) === lit(""))

}

object InclusionFilterController {
  private val log = org.apache.log4j.LogManager.getLogger(this.getClass)
  def getFilter(rcuGasMassivoPDF: DataFrame, rcuGasConnessioniDistr2DF: DataFrame): List[InclusionFilterController] = {
    val pdrFilter = new InclusionPdrFilter
    val distrFilter = new InclusionIdDistrFilter(rcuGasConnessioniDistr2DF)
    val distrPivaUdDFilter = new InclusionIdDistrPivaUdDFilter(rcuGasMassivoPDF, rcuGasConnessioniDistr2DF)

    var filters: List[InclusionFilterController] = List()
    if(pdrFilter.isEnabled) filters = filters ++ List(pdrFilter)
    if(distrFilter.isEnabled) filters = filters ++ List(distrFilter)
    if(distrPivaUdDFilter.isEnabled) filters = filters ++ List(distrPivaUdDFilter)

    log.info(s"Running with inclusion filters: $filters")

    filters
  }
}
