package it.eng.au.pubblicazione_cce.mock.writer

import it.eng.au.pubblicazione_cce.file.writer.FileWriter
import it.eng.au.pubblicazione_cce.model.file.FileModel
import org.apache.spark.sql.Dataset

class FileWriterMock(var ds: Dataset[FileModel] = null) extends FileWriter {

  override def write(ds: Dataset[FileModel]): Unit = {
    this.ds = ds
  }
}
