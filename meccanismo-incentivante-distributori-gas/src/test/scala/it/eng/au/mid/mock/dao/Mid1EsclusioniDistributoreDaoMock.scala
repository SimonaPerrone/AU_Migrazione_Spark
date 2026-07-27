package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.Mid1EsclusioniDistributoreDao
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniDistributoreModel
import org.apache.spark.sql.Dataset

class Mid1EsclusioniDistributoreDaoMock(var ds: Dataset[MidEsclusioniDistributoreModel]) extends Mid1EsclusioniDistributoreDao {
  override def read(): Dataset[MidEsclusioniDistributoreModel] = ds

}
