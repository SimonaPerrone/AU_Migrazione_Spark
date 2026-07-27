package it.eng.au.sgsFlussoStoricoGas.dao.dailyConsumption

import it.eng.au.sgsFlussoStoricoGas.schema.dailyConsumption.DailyConsumptionIncoerentiSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.DataFrame

class DailyConsumptionAggIncoerentiGdMDao extends AggDao {
  override val tablePath: String = Environment.getDailyConsumptionIncoerentiPath
  override val columns: List[String] = DailyConsumptionIncoerentiSchema.getValues
  override val tableName: String = Environment.getDailyConsumptionIncoerentiTableName

  override def readTable: DataFrame = {
    super.readTable
      .withColumnRenamed("pdr", DailyConsumptionIncoerentiSchema.pdrI)
      .withColumnRenamed("date", DailyConsumptionIncoerentiSchema.dateI)
      .withColumnRenamed("value", DailyConsumptionIncoerentiSchema.valueI)
      .withColumnRenamed("valueNotSterilized", DailyConsumptionIncoerentiSchema.valueNotSterilizedI)
      .selectExpr(columns:_*)
  }
}
