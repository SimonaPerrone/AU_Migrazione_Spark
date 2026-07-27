package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.sbg.DailyConsumptionSbgEsclusiDao
import it.eng.au.mid.model.flow.calcolo.DailyConsumptionEsclusiModel
import org.apache.spark.sql.Dataset

class DailyConsumptionSbgEsclusiDaoMock(ds: Dataset[DailyConsumptionEsclusiModel]) extends DailyConsumptionSbgEsclusiDao {
  override def read(): Dataset[DailyConsumptionEsclusiModel] = ds
}
