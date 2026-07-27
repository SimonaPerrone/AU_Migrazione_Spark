package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.calcolo.InclusioniAggDao
import it.eng.au.mid.model.file.calcolo.InclusioniModel
import org.apache.spark.sql.Dataset

class InclusioniAggDaoMock(ds: Dataset[InclusioniModel]) extends InclusioniAggDao {
  override def read(): Dataset[InclusioniModel] = ds
}
