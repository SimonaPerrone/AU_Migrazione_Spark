package it.eng.au.pubblicazione_cce.file.csv

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.schema.file.FileElencoFlussiSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat_ws, lit}

class ElencoFlussiCsvBuilder extends CceCsvBuilder {

  override val pivaCol: String = FileElencoFlussiSchema.piva
  override val processoCol: String = FileElencoFlussiSchema.processo
  override val ruoloCol: String = FileElencoFlussiSchema.ruolo

  override val maxLineCsv: Int = Environment.getOutputFileCsvMaxRow

  override val outputFilePath: String = Environment.getOutputFileTemporaryPath

  override val columnsFileRowContent: List[String] = List(
    FileElencoFlussiSchema.pod,
    FileElencoFlussiSchema.path_cloud,
    FileElencoFlussiSchema.annomese,
    FileElencoFlussiSchema.data_aggiornamento
  )

  override val headerCsv: Option[List[String]] = Some(columnsFileRowContent)

  override def computeFileName: Column = {
    concat_ws("_",
      col(FileElencoFlussiSchema.piva),
      lit(CostantiCCE.CCE),
      col(FileElencoFlussiSchema.processo),
      lit("Elenco_Flussi"),
      col(FileElencoFlussiSchema.annomese),
      lit(fileTimestamp),
      col(FileElencoFlussiSchema.id_richiesta)
    )
  }

  override val columnsFileGroup: List[String] = List(
    FileElencoFlussiSchema.sessione,
    FileElencoFlussiSchema.ruolo,
    FileElencoFlussiSchema.processo,
    FileElencoFlussiSchema.piva,
    FileElencoFlussiSchema.id_richiesta,
    FileElencoFlussiSchema.annomese
  )

}
