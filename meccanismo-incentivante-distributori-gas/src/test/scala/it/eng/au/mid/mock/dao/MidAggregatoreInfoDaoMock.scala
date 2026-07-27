package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.mid.MidAggregatoreInfoDao
import it.eng.au.mid.model.hive.mid.MidAggregatoreInfoModel
import org.apache.spark.sql.Dataset

class MidAggregatoreInfoDaoMock(var ds: Dataset[MidAggregatoreInfoModel]) extends MidAggregatoreInfoDao {
  override def read(): Dataset[MidAggregatoreInfoModel] = ds

  override def write(data: Dataset[MidAggregatoreInfoModel], overwrite: Boolean = true): Unit = {
    ds = data
  }
}
