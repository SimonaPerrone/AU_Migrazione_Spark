package it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.traits

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.RunnableAggregator
import it.eng.au.aggregatoreConsumiCdp.schema.OutputHiveSchema
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.{DATA_DECORRENZA_FORMAT, TIMESTAMP_FORMAT}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{Column, DataFrame}

trait Aggric extends RunnableAggregator {
  val operationName = "AGG_RIC"

  def fileSpecificFilterExpression: Column

  override def getAggregato(df: DataFrame): DataFrame = {
    var aggDF = df
    aggregatoColumns.foreach({ case (dailyName, fileName) =>
      aggDF = aggDF.withColumnRenamed(dailyName, fileName)
    })

    val aggFilter = aggDF
      .filter(fileSpecificFilterExpression && obligatoryExpression)
      .withColumn(OutputHiveSchema.data_decorrenza, from_unixtime(unix_timestamp(col(OutputHiveSchema.data_decorrenza), TIMESTAMP_FORMAT), DATA_DECORRENZA_FORMAT))

    //Only run cdp1 Udd override this method
    val splitUdd = splitUddSwitching(aggFilter)

    splitUdd
  }

  def splitUddSwitching(df: DataFrame): DataFrame = df
}
