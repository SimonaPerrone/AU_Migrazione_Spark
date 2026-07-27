package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.Mid2EsclusioniTrattamentoDao
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniTrattamentoModel
import org.apache.spark.sql.Dataset

class Mid2EsclusioniTrattamentoDaoMock(var ds: Dataset[MidEsclusioniTrattamentoModel]) extends Mid2EsclusioniTrattamentoDao {
  override def read(): Dataset[MidEsclusioniTrattamentoModel] = ds

}
