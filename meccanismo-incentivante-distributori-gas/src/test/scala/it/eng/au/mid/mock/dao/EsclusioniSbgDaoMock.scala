package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.calcolo.EsclusioniSbgDao
import it.eng.au.mid.model.file.calcolo.EsclusioniModel
import org.apache.spark.sql.Dataset

class EsclusioniSbgDaoMock(ds: Dataset[EsclusioniModel]) extends EsclusioniSbgDao {
  override def read(): Dataset[EsclusioniModel] = ds
}