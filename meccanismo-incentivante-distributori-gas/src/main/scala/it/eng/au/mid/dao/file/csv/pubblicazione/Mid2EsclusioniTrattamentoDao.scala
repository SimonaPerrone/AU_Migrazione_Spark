package it.eng.au.mid.dao.file.csv.pubblicazione

import it.eng.au.mid.dao.file.csv.CsvDao
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniTrattamentoModel
import it.eng.au.mid.schema.SchemaEnum
import it.eng.au.mid.schema.file.pubblicazione.MidEsclusioniTrattamentoSchema

class Mid2EsclusioniTrattamentoDao extends CsvDao[MidEsclusioniTrattamentoModel] {
  override val path: String = Environment.getProperty("file.path.mid2_esclusioni_tratt")
  override val schema: SchemaEnum = MidEsclusioniTrattamentoSchema
}
