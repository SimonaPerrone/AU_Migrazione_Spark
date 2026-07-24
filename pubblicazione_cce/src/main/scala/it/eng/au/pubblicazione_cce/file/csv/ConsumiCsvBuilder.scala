package it.eng.au.pubblicazione_cce.file.csv

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.schema.file.{FileConsumiHeaderSchema, FileConsumiSchema}
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit}

// Classe per definire il file CSV per i consumi
class ConsumiCsvBuilder extends CceCsvBuilder {

  override val pivaCol: String = FileConsumiSchema.piva
  override val processoCol: String = FileConsumiSchema.processo
  override val ruoloCol: String = FileConsumiSchema.ruolo

  override val maxLineCsv: Int = Environment.getOutputFileCsvMaxRow

  override val outputFilePath: String = Environment.getOutputFileTemporaryPath

  override val columnsFileRowContent: List[String] = List(
    FileConsumiSchema.data,
    FileConsumiSchema.cod_pod,
    FileConsumiSchema.piva_distr,
    FileConsumiSchema.piva_udd,
    FileConsumiSchema.h01, FileConsumiSchema.h02, FileConsumiSchema.h03, FileConsumiSchema.h04,
    FileConsumiSchema.h05, FileConsumiSchema.h06, FileConsumiSchema.h07, FileConsumiSchema.h08, FileConsumiSchema.h09,
    FileConsumiSchema.h10, FileConsumiSchema.h11, FileConsumiSchema.h12, FileConsumiSchema.h13, FileConsumiSchema.h14,
    FileConsumiSchema.h15, FileConsumiSchema.h16, FileConsumiSchema.h17, FileConsumiSchema.h18, FileConsumiSchema.h19,
    FileConsumiSchema.h20, FileConsumiSchema.h21, FileConsumiSchema.h22, FileConsumiSchema.h23, FileConsumiSchema.h24,
    FileConsumiSchema.h25,
    FileConsumiSchema.data_aggiornamento
  )

   val columnsFileRowContentHeader: List[String] = List(
     FileConsumiHeaderSchema.data,
     FileConsumiHeaderSchema.cod_pod,
     FileConsumiHeaderSchema.piva_distr,
     FileConsumiHeaderSchema.piva_udd,
     FileConsumiHeaderSchema.H01, FileConsumiHeaderSchema.H02, FileConsumiHeaderSchema.H03, FileConsumiHeaderSchema.H04,
     FileConsumiHeaderSchema.H05, FileConsumiHeaderSchema.H06, FileConsumiHeaderSchema.H07, FileConsumiHeaderSchema.H08, FileConsumiHeaderSchema.H09,
     FileConsumiHeaderSchema.H10, FileConsumiHeaderSchema.H11, FileConsumiHeaderSchema.H12, FileConsumiHeaderSchema.H13, FileConsumiHeaderSchema.H14,
     FileConsumiHeaderSchema.H15, FileConsumiHeaderSchema.H16, FileConsumiHeaderSchema.H17, FileConsumiHeaderSchema.H18, FileConsumiHeaderSchema.H19,
     FileConsumiHeaderSchema.H20, FileConsumiHeaderSchema.H21, FileConsumiHeaderSchema.H22, FileConsumiHeaderSchema.H23, FileConsumiHeaderSchema.H24,
     FileConsumiHeaderSchema.H25,
     FileConsumiHeaderSchema.data_aggiornamento
  )

  override val headerCsv: Option[List[String]] = Some(columnsFileRowContentHeader)

  override def computeFileName: Column = {
    concat_ws("_",
      col(FileConsumiSchema.piva),
      lit(CostantiCCE.CCE),
      col(FileConsumiSchema.processo),
      concat(col(FileConsumiSchema.anno), col(FileConsumiSchema.mese)),
      lit(fileTimestamp),
      col(FileConsumiSchema.id_richiesta)
    )
  }

  override val columnsFileGroup: List[String] = List(
    FileConsumiSchema.sessione,
    FileConsumiSchema.piva,
    FileConsumiSchema.ruolo,
    FileConsumiSchema.processo,
    FileConsumiSchema.id_richiesta,
    FileConsumiSchema.executionid,
    FileConsumiSchema.anno,
    FileConsumiSchema.mese
  )

}
