package it.eng.au.pubblicazione_cce.mock.writer

import it.eng.au.pubblicazione_cce.file.writer.ZipWriter
import it.eng.au.pubblicazione_cce.model.file.ZipFileModel
import it.eng.au.pubblicazione_cce.model.flow.EsitoConsumiModel
import org.apache.spark.sql.Dataset

import java.time.LocalDate

class ZipWriterMock(
                     override val processDate: LocalDate,
                     override val fileTimestamp: String
                   ) extends ZipWriter {

  import spark.implicits._

  override val MAX_BYTES_SIZE_ZIP: Long = 100000L

  override val outputFilePath: String = "/"

  override def write(ds: Dataset[ZipFileModel]): Dataset[EsitoConsumiModel] = {
    ds.map(r => EsitoConsumiModel(
      richiesta = r.id_richiesta,
      nZipFiles = 1,
      zipFiles = List(r.fileName + "_1.zip"),
      execution_id_input_read = null
    ))
  }

}
