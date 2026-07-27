package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.Mid2EsclusioniAnnomeseDao
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniAnnomeseModel
import org.apache.spark.sql.Dataset

class Mid2EsclusioniAnnomeseDaoMock(var ds: Dataset[MidEsclusioniAnnomeseModel]) extends Mid2EsclusioniAnnomeseDao {
  override def read(): Dataset[MidEsclusioniAnnomeseModel] = ds

}
