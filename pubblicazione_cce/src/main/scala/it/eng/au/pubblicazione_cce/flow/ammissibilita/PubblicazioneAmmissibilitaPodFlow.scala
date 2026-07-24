package it.eng.au.pubblicazione_cce.flow.ammissibilita

import it.eng.au.pubblicazione_cce.common.CostantiCCE
import it.eng.au.pubblicazione_cce.file.csv.{AmmissibilitaPodCsvBuilder, DataFrameCsvBuilder}
import it.eng.au.pubblicazione_cce.model.cce.CceEsitoModel
import it.eng.au.pubblicazione_cce.model.file.FileModel
import it.eng.au.pubblicazione_cce.schema.cce.CceRichiestaPodSchema
import it.eng.au.pubblicazione_cce.schema.file.FileSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment
import org.apache.spark.sql.{DataFrame, Dataset}

import java.time.LocalDate

class PubblicazioneAmmissibilitaPodFlow(val dataRichieste: LocalDate, val processo: String) extends PubblicazioneAmmissibilitaFlow {

  import spark.implicits._

  override val tipo: String = CostantiCCE.RICHIESTA_POD_POD

  override val csvBuilder: DataFrameCsvBuilder = new AmmissibilitaPodCsvBuilder

  override def calcolaEsitoAmmissibilita(df: DataFrame): Dataset[CceEsitoModel] = {
    df.map(r => {
      val fileModel = FileModel(
        filePathRoot = r.getAs[String](FileSchema.filePathRoot),
        filePathSubDirectories = Option(r.getAs[String](FileSchema.filePathSubDirectories)),
        fileName = r.getAs[String](FileSchema.fileName),
        fileFullName = null,
        fileContent = null
      )
      CceEsitoModel(
        n_id_richiesta = r.getAs[String](CceRichiestaPodSchema.n_id_richiesta),
        t_path = fileModel.filePath,
        t_file_ammissibilita = fileModel.fileName,
        t_stato = null,
        t_operation_name = null,
        tipo_richiesta = CostantiCCE.RICHIESTA_POD,
        n_executionid = null,
        d_data_esito = null,
        d_data_richiesta = null
      )
    })
  }

}
