package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.mid.MidContatoriDao
import it.eng.au.mid.model.hive.mid.MidContatoriModel
import org.apache.spark.sql.Dataset

class MidContatoriDaoMock(var ds: Dataset[MidContatoriModel]) extends MidContatoriDao {
  override def read(): Dataset[MidContatoriModel] = ds

  override def write(data: Dataset[MidContatoriModel], overwrite: Boolean = true): Unit = {
    ds = data
  }
}
