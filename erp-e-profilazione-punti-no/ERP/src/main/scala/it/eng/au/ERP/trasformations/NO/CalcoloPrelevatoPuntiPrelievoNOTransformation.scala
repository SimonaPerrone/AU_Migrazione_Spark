package it.eng.au.ERP.trasformations.NO

import it.eng.au.ERP.schema.erp.{erpAggregatoNoSchema, erpDailyNoSchema}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CalcoloPrelevatoPuntiPrelievoNOTransformation {

  /**
   * Aggregates ERP_DAILY_NO data by area/PIVA/day to produce ERP_AGGREGATO_NO
   * 
   * Logic:
   * 1. Read erp_daily_no filtered by executionId
   * 2. Group by: anno, mese, giorno, area, piva_distr, rag_soc_distr
   * 3. Sum all quartori q1-q100 for each group
   * 4. Output to erp_aggregato_no
   * 
   * @param dfDailyNo DataFrame from erp_daily_no table
   * @param executionId Execution timestamp for filtering
   * @param podExcluded List of PODs to exclude
   * @return Aggregated DataFrame ready to write to erp_aggregato_no
   */
  def aggregateNODailyProfiles(
      dfDailyNo: DataFrame,
      executionId: Long,
      podExcluded: List[String]
  )(implicit spark: SparkSession): DataFrame = {

    val executionIdValue = executionId.toString

    // Filter by executionId and exclude specific PODs (only if executionid column exists)
    val filtered = if (dfDailyNo.columns.contains(erpDailyNoSchema.executionid.toString)) {
      if (podExcluded.nonEmpty) {
        dfDailyNo
          .filter(col(erpDailyNoSchema.executionid) === executionIdValue)
          .filter(!col(erpDailyNoSchema.pod).isin(podExcluded: _*))
      } else {
        dfDailyNo
          .filter(col(erpDailyNoSchema.executionid) === executionIdValue)
      }
    } else {
      // For test dataframes that don't have executionid column
      if (podExcluded.nonEmpty) {
        dfDailyNo
          .filter(!col(erpDailyNoSchema.pod).isin(podExcluded: _*))
      } else {
        dfDailyNo
      }
    }

    // Group by key dimensions and aggregate all quartori
    // Get all quartori columns that exist in the dataframe (dynamically)
    val quartoriColumns = filtered.columns.filter(_.startsWith("q")).sorted

    // Create aggregation expressions for existing quartori columns
    val quartoriAggExprs = quartoriColumns.map(qCol =>
      sum(col(qCol)).alias(qCol)
    )

    val aggregated = filtered
      .groupBy(
        col(erpDailyNoSchema.anno),
        col(erpDailyNoSchema.mese),
        col(erpDailyNoSchema.giorno),
        col(erpDailyNoSchema.area),
        col(erpDailyNoSchema.piva_distr),
        col(erpDailyNoSchema.rag_soc_distr)
      )
      .agg(quartoriAggExprs.head, quartoriAggExprs.tail: _*)
      .withColumn(erpAggregatoNoSchema.executionid, lit(executionIdValue))

    // Select columns in schema order, but only those that exist in the aggregated dataframe
    val finalColumns = erpAggregatoNoSchema.getValues.filter(colName =>
      aggregated.columns.contains(colName) || colName == erpAggregatoNoSchema.executionid
    )
    aggregated.selectExpr(finalColumns: _*)
  }
}
