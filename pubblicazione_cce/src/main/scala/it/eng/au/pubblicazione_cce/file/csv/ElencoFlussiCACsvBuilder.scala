package it.eng.au.pubblicazione_cce.file.csv

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.schema.file.FileElencoFlussiCaSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat_ws, lit}

class ElencoFlussiCACsvBuilder extends CceCsvBuilder {

  override val pivaCol: String = FileElencoFlussiCaSchema.piva
  override val processoCol: String = FileElencoFlussiCaSchema.processo
  override val ruoloCol: String = FileElencoFlussiCaSchema.ruolo

  override val maxLineCsv: Int = Environment.getOutputFileCsvMaxRow

  override val outputFilePath: String = Environment.getOutputFileTemporaryPath

  override val columnsFileRowContent: List[String] = List(
    FileElencoFlussiCaSchema.pod,
    FileElencoFlussiCaSchema.path_cloud
  )

  override val headerCsv: Option[List[String]] = Some(columnsFileRowContent)

  override def computeFileName: Column = {
    concat_ws("_",
      col(FileElencoFlussiCaSchema.piva),
      lit(CostantiCCE.CCE),
      col(FileElencoFlussiCaSchema.processo),
      lit("Elenco_Flussi"),
      col(FileElencoFlussiCaSchema.anno),
      lit(fileTimestamp),
      col(FileElencoFlussiCaSchema.id_richiesta)
    )
  }

  override val columnsFileGroup: List[String] = List(
    FileElencoFlussiCaSchema.sessione,
    FileElencoFlussiCaSchema.ruolo,
    FileElencoFlussiCaSchema.processo,
    FileElencoFlussiCaSchema.piva,
    FileElencoFlussiCaSchema.id_richiesta,
    FileElencoFlussiCaSchema.anno
  )

}
