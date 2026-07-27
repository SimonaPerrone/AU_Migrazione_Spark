package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.agg.DailyConsumptionAggEsclusiDao
import it.eng.au.mid.model.flow.calcolo.DailyConsumptionEsclusiModel
import org.apache.spark.sql.Dataset

class DailyConsumptionAggEsclusiDaoMock(ds: Dataset[DailyConsumptionEsclusiModel]) extends DailyConsumptionAggEsclusiDao {
  override def read(): Dataset[DailyConsumptionEsclusiModel] = ds
}
