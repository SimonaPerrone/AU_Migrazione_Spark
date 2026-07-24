package it.eng.au.pubblicazione_cce.mock.dao

import it.eng.au.pubblicazione_cce.dao.cce.CceCalcTrackDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcTrackModel
import org.apache.spark.sql.Dataset

class CceCalcTrackDaoMock(var ds: Dataset[CceCalcTrackModel]) extends CceCalcTrackDao {

  override def read(): Dataset[CceCalcTrackModel] = ds
}
