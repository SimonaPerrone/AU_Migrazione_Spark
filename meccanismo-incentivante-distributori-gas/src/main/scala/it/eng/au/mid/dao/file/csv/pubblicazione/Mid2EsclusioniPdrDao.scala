package it.eng.au.mid.dao.file.csv.pubblicazione

import it.eng.au.mid.dao.file.csv.CsvDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniPdrModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.file.pubblicazione.MidEsclusioniPdrSchema

class Mid2EsclusioniPdrDao extends CsvDao[MidEsclusioniPdrModel] {
  override val path: String = Environment.getProperty("file.path.mid2_esclusioni_pdr")
  override val schema: SchemaEnum = MidEsclusioniPdrSchema
}
