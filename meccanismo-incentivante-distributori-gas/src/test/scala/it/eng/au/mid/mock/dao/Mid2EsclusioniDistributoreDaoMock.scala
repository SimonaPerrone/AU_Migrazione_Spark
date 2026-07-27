package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.Mid2EsclusioniDistributoreDao
import it.eng.au.mid.model.file.pubblicazione.MidEsclusioniDistributoreModel
import org.apache.spark.sql.Dataset

class Mid2EsclusioniDistributoreDaoMock(var ds: Dataset[MidEsclusioniDistributoreModel]) extends Mid2EsclusioniDistributoreDao {
  override def read(): Dataset[MidEsclusioniDistributoreModel] = ds

}
