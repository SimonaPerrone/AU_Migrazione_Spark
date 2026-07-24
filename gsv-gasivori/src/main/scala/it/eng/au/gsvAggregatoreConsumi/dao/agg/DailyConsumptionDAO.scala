package it.eng.au.gsvAggregatoreConsumi.dao.agg

import it.eng.au.gsvAggregatoreConsumi.schema.agg.DailyConsumptionSchema
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment

class DailyConsumptionDAO extends AggDao {
  override val tableName: String = Environment.getDailyConsumptionTable
  override val tablePath: String = Environment.getDailyConsumptionPath
  override val columns: List[String] = DailyConsumptionSchema.getValues

}
