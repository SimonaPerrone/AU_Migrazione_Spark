package it.au.misure.ingestionMisureGasUnico.utility

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, trim}

object DataFrameUtility {
  /***
   * Return dataframe with trimmed columns.
   * Input Df must have all String columns
   */
  def trimColumns(df: DataFrame, schema: List[String]): DataFrame = {
    var result = df
    for(column <- schema){
      result = result.withColumn(column, trim(col(column)))
    }
    result
  }

}
