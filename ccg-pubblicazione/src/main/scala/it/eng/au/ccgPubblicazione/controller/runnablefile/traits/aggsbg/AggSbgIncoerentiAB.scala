package it.eng.au.ccgPubblicazione.controller.runnablefile.traits.aggsbg

import it.eng.au.ccgPubblicazione.schema.aggsbg.AggConsumptionRequestRunnableSchema
import it.eng.au.ccgPubblicazione.utility.Constants.INCOERENTI
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

trait AggSbgIncoerentiAB extends AggSbgPdrElencoFlussi {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  override val publicationType: String = INCOERENTI

  /**
   * Esegue i filtri e le operazioni al fine di individuare i PdR incoerenti AB a partire dal dataframe dei consumi [[df]].
   * @param df dataframe dei consumi
   * @return [[df]] contenente soltanto i PdR incoerenti AB
   */
  override def consumptionFilter(df: DataFrame): DataFrame = {
    val monthlyConsumptionColName = "monthly_consumption"
    val isAnomalousColName = "is_anomalous"
    val hasAnomalyInMonthColName = "has_anomaly_in_month"
    val anomalousConditionExpression = (// pdr con consumo giornaliero > del 30% del consumo mensile e ....
      (col(monthlyConsumptionColName) * 0.3 < col(AggConsumptionRequestRunnableSchema.value)) and (
        (col(AggConsumptionRequestRunnableSchema.ca) >= lit(5000) and col(monthlyConsumptionColName) >= lit(5000)) or
          (col(AggConsumptionRequestRunnableSchema.ca) < lit(5000) and col(monthlyConsumptionColName) > lit(100000))
        )
      ) or (// consumo mensile maggiore della ca e ...
      col(monthlyConsumptionColName) > col(AggConsumptionRequestRunnableSchema.ca) and
        (col(AggConsumptionRequestRunnableSchema.ca) >= lit(5000) or
          col(monthlyConsumptionColName) > lit(100000)
          )
      )

    val aggDF = df
      .na.fill("Y", Seq(AggConsumptionRequestRunnableSchema.treatment.toString))
      .filter(col(AggConsumptionRequestRunnableSchema.errorCode).isin(0, 10, 11, 12) and
        not(col(AggConsumptionRequestRunnableSchema.forceExclusion) <=> true) and
        col(AggConsumptionRequestRunnableSchema.isValid) === true and
        col(AggConsumptionRequestRunnableSchema.pivaUdd).isNotNull and
        col(AggConsumptionRequestRunnableSchema.dtg).isNotNull and
        col(AggConsumptionRequestRunnableSchema.codRemi).isNotNull and
        col(AggConsumptionRequestRunnableSchema.codProfStd).isNotNull and
        col(AggConsumptionRequestRunnableSchema.tipoCliente).isNotNull and
        col(AggConsumptionRequestRunnableSchema.unitMisPrel).isNotNull and
        col(AggConsumptionRequestRunnableSchema.treatment).isin("G", "M") and
        fileSpecificFilterExpression)
      //we only take PdRs which are not GDM anomalous
      .where(not(col(AggConsumptionRequestRunnableSchema.isPdrAnomalousGDM) <=> true))
      .withColumn(monthlyConsumptionColName, sum(AggConsumptionRequestRunnableSchema.value).over(Window.partitionBy(col(AggConsumptionRequestRunnableSchema.pdr), col(AggConsumptionRequestRunnableSchema.annoMese))))
      .withColumn(isAnomalousColName, when(anomalousConditionExpression, true).otherwise(false))
      //take consumptions for all the months if at least one day is anomalous
      .withColumn(hasAnomalyInMonthColName, max(col(isAnomalousColName)).over(Window.partitionBy(col(AggConsumptionRequestRunnableSchema.pdr), col(AggConsumptionRequestRunnableSchema.annoMese))))
      .where(col(hasAnomalyInMonthColName) === true)
      .drop(
        monthlyConsumptionColName
        , isAnomalousColName
        , hasAnomalyInMonthColName
      )

    aggDF
  }
}
