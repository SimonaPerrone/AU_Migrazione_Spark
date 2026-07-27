package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.Mid1EsclusioniTrattamentoDao
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniTrattamentoModel
import org.apache.spark.sql.Dataset

class Mid1EsclusioniTrattamentoDaoMock(var ds: Dataset[MidEsclusioniTrattamentoModel]) extends Mid1EsclusioniTrattamentoDao {
  override def read(): Dataset[MidEsclusioniTrattamentoModel] = ds

}
