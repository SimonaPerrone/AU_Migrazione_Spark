package it.eng.au.sgsFlussoStoricoGas.dao.dailyConsumption

import it.eng.au.sgsFlussoStoricoGas.schema.dailyConsumption.DailyConsumptionEsclusiSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame

class DailyConsumptionAggEsclusiDao extends AggDao {
  override val tablePath: String = Environment.getDailyConsumptionEsclusiPath
  override val columns: List[String] = DailyConsumptionEsclusiSchema.getValues
  override val tableName: String = Environment.getDailyConsumptionEsclusiTableName

  override def readTable: DataFrame = {
    super.readTable
      .withColumnRenamed("pdr", DailyConsumptionEsclusiSchema.pdrE)
      .withColumnRenamed("date", DailyConsumptionEsclusiSchema.dateE)
      .withColumnRenamed("value", DailyConsumptionEsclusiSchema.valueE)
      .withColumnRenamed("valueNotSterilized", DailyConsumptionEsclusiSchema.valueNotSterilizedE)
      .selectExpr(columns:_*)
  }
}
