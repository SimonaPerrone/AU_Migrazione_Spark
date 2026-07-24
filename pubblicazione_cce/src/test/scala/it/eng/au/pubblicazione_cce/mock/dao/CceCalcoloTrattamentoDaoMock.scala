package it.eng.au.pubblicazione_cce.mock.dao

import it.eng.au.pubblicazione_cce.dao.cce.CceCalcoloTrattamentoDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloTrattamentoModel
import org.apache.spark.sql.Dataset

class CceCalcoloTrattamentoDaoMock(var ds: Dataset[CceCalcoloTrattamentoModel]) extends CceCalcoloTrattamentoDao {
  override def read(): Dataset[CceCalcoloTrattamentoModel] = ds

}
