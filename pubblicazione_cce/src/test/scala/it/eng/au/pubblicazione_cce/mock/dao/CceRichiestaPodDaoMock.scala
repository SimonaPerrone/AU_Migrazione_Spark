package it.eng.au.pubblicazione_cce.mock.dao

import it.eng.au.pubblicazione_cce.dao.cce.CceRichiestaPodDao
import it.eng.au.pubblicazione_cce.model.cce.CceRichiestaPodModel
import org.apache.spark.sql.Dataset

class CceRichiestaPodDaoMock(var ds: Dataset[CceRichiestaPodModel]) extends CceRichiestaPodDao {

  override def read(): Dataset[CceRichiestaPodModel] = ds
}
