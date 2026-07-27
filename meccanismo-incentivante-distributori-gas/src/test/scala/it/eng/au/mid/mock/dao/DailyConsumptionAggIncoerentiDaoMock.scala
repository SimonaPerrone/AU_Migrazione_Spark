package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.agg.DailyConsumptionAggIncoerentiDao
import it.eng.au.mid.model.flow.calcolo.DailyConsumptionIncoerentiModel
import org.apache.spark.sql.Dataset

class DailyConsumptionAggIncoerentiDaoMock(ds: Dataset[DailyConsumptionIncoerentiModel]) extends DailyConsumptionAggIncoerentiDao {
  override def read(): Dataset[DailyConsumptionIncoerentiModel] = ds
}
