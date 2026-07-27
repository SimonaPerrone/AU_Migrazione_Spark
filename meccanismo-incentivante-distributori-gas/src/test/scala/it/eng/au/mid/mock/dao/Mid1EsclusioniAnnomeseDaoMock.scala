package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.Mid1EsclusioniAnnomeseDao
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniAnnomeseModel
import org.apache.spark.sql.Dataset

class Mid1EsclusioniAnnomeseDaoMock(var ds: Dataset[MidEsclusioniAnnomeseModel]) extends Mid1EsclusioniAnnomeseDao {
  override def read(): Dataset[MidEsclusioniAnnomeseModel] = ds

}
