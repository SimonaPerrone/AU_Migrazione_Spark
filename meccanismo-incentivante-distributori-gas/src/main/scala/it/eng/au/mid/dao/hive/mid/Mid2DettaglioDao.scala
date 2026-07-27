package it.eng.au.mid.dao.hive.mid

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.hive.mid.Mid2DettaglioModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.hive.mid.Mid2DettaglioSchema

class Mid2DettaglioDao extends HiveDao[Mid2DettaglioModel] {
  override val tableName: String = Environment.getProperty("hive.table.mid2_dettaglio")
  override val schema: SchemaEnum = Mid2DettaglioSchema
}
