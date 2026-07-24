package it.eng.au.pubblicazione_cce.mock.dao

import it.eng.au.pubblicazione_cce.dao.cce.CceEsitoDao
import it.eng.au.pubblicazione_cce.model.cce.CceEsitoModel
import org.apache.spark.sql.Dataset

class CceEsitoDaoMock(var ds: Dataset[CceEsitoModel] = null) extends CceEsitoDao {

  override def read(): Dataset[CceEsitoModel] = ds

  override def write(data: Dataset[CceEsitoModel], overwrite: Boolean): Unit = {
    ds = data
  }
}
