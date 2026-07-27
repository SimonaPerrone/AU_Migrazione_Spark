package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.hive.atg.AtgVariazioniSocDao
import it.eng.au.mid.model.hive.atg.AtgVariazioniSocModel
import org.apache.spark.sql.{DataFrame, Dataset}

class AtgVariazioniSocDaoMock(ds: Dataset[AtgVariazioniSocModel]) extends AtgVariazioniSocDao {
  override def read(): Dataset[AtgVariazioniSocModel] = ds
}
