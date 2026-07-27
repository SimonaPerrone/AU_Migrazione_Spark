package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.mid.Mid1DettaglioDao
import it.eng.au.mid.model.hive.mid.Mid1DettaglioModel
import org.apache.spark.sql.Dataset

class Mid1DettaglioDaoMock(var ds: Dataset[Mid1DettaglioModel]) extends Mid1DettaglioDao {
  override def read(): Dataset[Mid1DettaglioModel] = ds

  override def write(data: Dataset[Mid1DettaglioModel], overwrite: Boolean = true): Unit = {
    ds = data
  }
}
