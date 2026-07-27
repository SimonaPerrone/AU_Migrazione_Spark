package it.eng.au.mid.dao.hive.sbg

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.flow.calcolo.DailyConsumptionEsclusiModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionEsclusiSchema

class DailyConsumptionSbgEsclusiDao extends HiveDao[DailyConsumptionEsclusiModel] {
  override val tableName: String = Environment.getProperty("hive.table.sbg_esclusi")
  override val schema: SchemaEnum = DailyConsumptionEsclusiSchema
}
