package it.eng.au.mid.dao.hive.mid

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.hive.mid.Mid1DettaglioModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.hive.mid.Mid1DettaglioSchema

class Mid1DettaglioDao extends HiveDao[Mid1DettaglioModel] {
  override val tableName: String = Environment.getProperty("hive.table.mid1_dettaglio")
  override val schema: SchemaEnum = Mid1DettaglioSchema
}
