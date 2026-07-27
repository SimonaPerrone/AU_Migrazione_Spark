package it.eng.au.mid.dao.hive.agg

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.flow.calcolo.DailyConsumptionIncoerentiModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionIncoerentiSchema

class DailyConsumptionAggIncoerentiDao extends HiveDao[DailyConsumptionIncoerentiModel] {
  override val tableName: String = Environment.getProperty("hive.table.agg_incoerenti")
  override val schema: SchemaEnum = DailyConsumptionIncoerentiSchema
}
