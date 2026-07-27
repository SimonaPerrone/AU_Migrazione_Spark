package it.eng.au.mid.mock.dao

import it.eng.au.mid.dao.file.zip.ZipWriterDao
import it.eng.au.mid.model.file.pubblicazione.ZipWriterModel
import org.apache.spark.sql.Dataset

class ZipWriterDaoMock(var ds: Dataset[ZipWriterModel] = null) extends ZipWriterDao {

  override def write(ds: Dataset[ZipWriterModel]): Unit = {
    this.ds = ds
  }
}
