package it.eng.au.sgsFlussoStoricoGas.dao.dailyConsumption

import it.eng.au.sgsFlussoStoricoGas.schema.dailyConsumption.DailyConsumptionSchema
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment

class DailyConsumptionAggDao extends AggDao {
  override val tablePath: String = Environment.getDailyConsumptionPath
  override val columns: List[String] = DailyConsumptionSchema.getValues
  override val tableName: String = Environment.getDailyConsumptionTableName
}
