package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.sbg.DailyConsumptionSbgIncoerentiDao
import it.eng.au.mid.model.flow.calcolo.DailyConsumptionIncoerentiModel
import org.apache.spark.sql.Dataset

class DailyConsumptionSbgIncoerentiDaoMock(ds: Dataset[DailyConsumptionIncoerentiModel]) extends DailyConsumptionSbgIncoerentiDao {
  override def read(): Dataset[DailyConsumptionIncoerentiModel] = ds
}
