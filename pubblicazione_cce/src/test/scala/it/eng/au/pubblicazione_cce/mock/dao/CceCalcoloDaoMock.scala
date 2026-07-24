package it.eng.au.pubblicazione_cce.mock.dao

import it.eng.au.pubblicazione_cce.dao.cce.CceCalcoloDao
import it.eng.au.pubblicazione_cce.model.cce.CceCalcoloMisureModel
import org.apache.spark.sql.{DataFrame, Dataset}

class CceCalcoloDaoMock(var ds: Dataset[CceCalcoloMisureModel]) extends CceCalcoloDao {
  override val tableName: String = ""

  override def readDF(): DataFrame = ds.toDF()

}
