package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.csv.pubblicazione.MidAlphaValoriDao
import it.eng.au.mid.model.file.pubblicazione.MidAlphaValoriModel
import org.apache.spark.sql.Dataset


class MidAlphaValoriDaoMock(var ds: Dataset[MidAlphaValoriModel]) extends MidAlphaValoriDao {
  override def read(): Dataset[MidAlphaValoriModel] = ds
}
