package it.eng.au.mid.dao.hive.atg

import it.eng.au.mid.dao.hive.HiveDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.hive.atg.AtgVariazioniSocModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.hive.atg.AtgVariazioniSocSchema

class AtgVariazioniSocDao extends HiveDao[AtgVariazioniSocModel] {
  override val tableName: String = Environment.getProperty("hive.table.variazioni_soc")
  override val schema: SchemaEnum = AtgVariazioniSocSchema
}
