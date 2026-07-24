package it.eng.au.pubblicazione_cce.file.csv

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.schema.file.{FileConsumiCaHeaderSchema, FileConsumiCaSchema}
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat_ws, lit}

//TODO: da implementare per CA
class ConsumiCaCsvBuilder extends CceCsvBuilder {

  override val pivaCol: String = FileConsumiCaSchema.piva
  override val processoCol: String = FileConsumiCaSchema.processo
  override val ruoloCol: String = FileConsumiCaSchema.ruolo

  override val maxLineCsv: Int = Environment.getOutputFileCsvMaxRow

  override val outputFilePath: String = Environment.getOutputFileTemporaryPath

  override val columnsFileRowContent: List[String] = List(
    FileConsumiCaSchema.anno,
    FileConsumiCaSchema.cod_pod,
    FileConsumiCaSchema.piva_distr,
    FileConsumiCaSchema.piva_udd,
    FileConsumiCaSchema.ca,
    FileConsumiCaSchema.data_aggiornamento
  )

  val columnsFileRowHeader: List[String] = List(
    FileConsumiCaHeaderSchema.data,
    FileConsumiCaHeaderSchema.cod_pod,
    FileConsumiCaHeaderSchema.piva_distr,
    FileConsumiCaHeaderSchema.piva_udd,
    FileConsumiCaHeaderSchema.ca,
    FileConsumiCaHeaderSchema.data_aggiornamento
  )

  override val headerCsv: Option[List[String]] = Some(columnsFileRowHeader)

  override def computeFileName: Column = {
    concat_ws("_",
      col(FileConsumiCaSchema.piva),
      lit(CostantiCCE.CCE),
      col(FileConsumiCaSchema.processo),
      col(FileConsumiCaSchema.anno),
      lit(fileTimestamp),
      col(FileConsumiCaSchema.id_richiesta)
    )

  }

  override val columnsFileGroup: List[String] = List(
    FileConsumiCaSchema.sessione,
    FileConsumiCaSchema.piva,
    FileConsumiCaSchema.ruolo,
    FileConsumiCaSchema.processo,
    FileConsumiCaSchema.id_richiesta,
    FileConsumiCaSchema.executionid,
    FileConsumiCaSchema.anno
  )

}
