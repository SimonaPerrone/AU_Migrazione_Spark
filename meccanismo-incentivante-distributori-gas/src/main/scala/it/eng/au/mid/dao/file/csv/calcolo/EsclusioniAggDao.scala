package it.eng.au.mid.dao.file.csv.calcolo

import it.eng.au.mid.dao.file.csv.CsvDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.calcolo.EsclusioniModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.file.calcolo.EsclusioniSchema

class EsclusioniAggDao extends CsvDao[EsclusioniModel] {
  override val path: String = Environment.getProperty("file.path.esclusioni_agg")
  override val schema: SchemaEnum = EsclusioniSchema
}
