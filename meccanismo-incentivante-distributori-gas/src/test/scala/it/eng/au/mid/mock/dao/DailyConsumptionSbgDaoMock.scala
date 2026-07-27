package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.sbg.DailyConsumptionSbgDao
import it.eng.au.mid.model.flow.DailyConsumptionModel
import org.apache.spark.sql.{DataFrame, Dataset}

class DailyConsumptionSbgDaoMock(ds: Dataset[DailyConsumptionModel]) extends DailyConsumptionSbgDao {
  override def readDF(): DataFrame = ds.toDF()
}
