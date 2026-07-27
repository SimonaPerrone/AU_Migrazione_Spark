package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.Mid1EsclusioniPdrDao
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniPdrModel
import org.apache.spark.sql.Dataset

class Mid1EsclusioniPdrDaoMock(var ds: Dataset[MidEsclusioniPdrModel]) extends Mid1EsclusioniPdrDao {
  override def read(): Dataset[MidEsclusioniPdrModel] = ds

}
