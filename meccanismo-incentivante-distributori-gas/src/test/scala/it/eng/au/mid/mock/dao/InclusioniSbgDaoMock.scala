package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.calcolo.InclusioniSbgDao
import it.eng.au.mid.model.file.calcolo.InclusioniModel
import org.apache.spark.sql.Dataset

class InclusioniSbgDaoMock(ds: Dataset[InclusioniModel]) extends InclusioniSbgDao {
  override def read(): Dataset[InclusioniModel] = ds
}
