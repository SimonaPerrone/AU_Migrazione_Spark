package it.eng.au.pubblicazione_cce.file.csv

import it.eng.au.pubblicazione_cce.schema.cce.CceRichiestaPodSchema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, concat_ws, lit}

class AmmissibilitaFileCsvBuilder extends CceCsvBuilder {

  override val pivaCol: String = CceRichiestaPodSchema.t_piva
  override val processoCol: String = CceRichiestaPodSchema.t_processo
  override val ruoloCol: String = CceRichiestaPodSchema.t_ruolo

  override val headerCsv: Option[List[String]] = Some(List("File", "Ammissibilità", "Codice_Inammissibilità", "Descrizione"))

  override val columnsFileGroup: List[String] = List(
    CceRichiestaPodSchema.t_servizio,
    CceRichiestaPodSchema.t_processo,
    CceRichiestaPodSchema.t_piva,
    CceRichiestaPodSchema.n_id_richiesta,
    CceRichiestaPodSchema.t_ruolo
  )

  override def computeFileName: Column = {
    concat_ws("_",
      lit("ReportEsitoFileContatoreConsumi"),
      col(CceRichiestaPodSchema.n_id_richiesta),
      lit(fileTimestamp))
  }


  override val columnsFileRowContent: List[String] = List(
    CceRichiestaPodSchema.t_nome_file,
    CceRichiestaPodSchema.b_ammissibilita,
    CceRichiestaPodSchema.t_cod_causale,
    CceRichiestaPodSchema.t_motivazione
  )

}
