package it.eng.au.mid.dao.hive.mid

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.hive.mid.MidAggregatoreInfoModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.hive.mid.MidAggregatoreInfoSchema

class MidAggregatoreInfoDao extends HiveDao[MidAggregatoreInfoModel] {
  override val tableName: String = Environment.getProperty("hive.table.mid_aggregatore_info")
  override val schema: SchemaEnum = MidAggregatoreInfoSchema
}
