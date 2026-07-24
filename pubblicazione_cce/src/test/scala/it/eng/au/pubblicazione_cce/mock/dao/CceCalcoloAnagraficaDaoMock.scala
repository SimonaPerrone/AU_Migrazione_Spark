package it.eng.au.pubblicazione_cce.mock.dao

import it.eng.au.pubblicazione_cce.dao.cce.CceCalcoloAnagraficaDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloAnagraficaModel
import org.apache.spark.sql.Dataset

class CceCalcoloAnagraficaDaoMock(var ds: Dataset[CceCalcoloAnagraficaModel]) extends CceCalcoloAnagraficaDao {
  override def read(): Dataset[CceCalcoloAnagraficaModel] = ds

}
