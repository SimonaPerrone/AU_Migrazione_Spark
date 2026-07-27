package it.eng.au.mid.dao.file.csv.pubblicazione

import it.eng.au.mid.dao.file.csv.CsvDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniDistributoreModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.file.pubblicazione.MidEsclusioniDistributoreSchema

class Mid2EsclusioniDistributoreDao extends CsvDao[MidEsclusioniDistributoreModel] {
  override val path: String = Environment.getProperty("file.path.mid2_esclusioni_distr")
  override val schema: SchemaEnum = MidEsclusioniDistributoreSchema
}
