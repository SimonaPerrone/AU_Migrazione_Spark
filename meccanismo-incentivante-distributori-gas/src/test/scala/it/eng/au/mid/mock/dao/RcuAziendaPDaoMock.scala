package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.rcu.RcuAziendaPDao
import it.eng.au.mid.model.hive.rcu.RcuAziendaPModel
import org.apache.spark.sql.Dataset

class RcuAziendaPDaoMock(ds: Dataset[RcuAziendaPModel]) extends RcuAziendaPDao {
  override def read(): Dataset[RcuAziendaPModel] = ds
}
