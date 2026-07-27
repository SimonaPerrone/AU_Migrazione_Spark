package it.eng.au.mid.dao.file.csv.pubblicazione

import it.eng.au.mid.dao.file.csv.CsvDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniAnnomeseModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.file.pubblicazione.MidEsclusioniAnnomeseSchema

class Mid2EsclusioniAnnomeseDao extends CsvDao[MidEsclusioniAnnomeseModel] {
  override val path: String = Environment.getProperty("file.path.mid2_esclusioni_annomese")
  override val schema: SchemaEnum = MidEsclusioniAnnomeseSchema
}
