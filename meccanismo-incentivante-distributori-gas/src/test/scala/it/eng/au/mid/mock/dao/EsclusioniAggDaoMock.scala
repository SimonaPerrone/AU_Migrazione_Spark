package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.calcolo.EsclusioniAggDao
import it.eng.au.mid.model.file.calcolo.EsclusioniModel
import org.apache.spark.sql.Dataset

class EsclusioniAggDaoMock(ds: Dataset[EsclusioniModel]) extends EsclusioniAggDao {
  override def read(): Dataset[EsclusioniModel] = ds
}