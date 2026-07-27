package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.agg.DailyConsumptionAggDao
import it.eng.au.mid.model.flow.DailyConsumptionModel
import org.apache.spark.sql.{DataFrame, Dataset}

class DailyConsumptionAggDaoMock(ds: Dataset[DailyConsumptionModel]) extends DailyConsumptionAggDao {
  override def readDF(): DataFrame = ds.toDF()
}
