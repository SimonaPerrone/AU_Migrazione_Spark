package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.mid.Mid2DettaglioDao
import it.eng.au.mid.model.hive.mid.Mid2DettaglioModel
import org.apache.spark.sql.Dataset

class Mid2DettaglioDaoMock(var ds: Dataset[Mid2DettaglioModel]) extends Mid2DettaglioDao {
  override def read(): Dataset[Mid2DettaglioModel] = ds

  override def write(data: Dataset[Mid2DettaglioModel], overwrite: Boolean = true): Unit = {
    ds = data
  }
}
