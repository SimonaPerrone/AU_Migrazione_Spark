package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.rcugas.RcugasConnessioniDistr2RemiPDao
import it.eng.au.mid.model.hive.rcugas.RcugasConnessioniDistr2RemiPModel
import org.apache.spark.sql.Dataset

class RcugasConnessioniDistr2RemiPDaoMock(ds: Dataset[RcugasConnessioniDistr2RemiPModel]) extends RcugasConnessioniDistr2RemiPDao {
  override def read(): Dataset[RcugasConnessioniDistr2RemiPModel] = ds
}
