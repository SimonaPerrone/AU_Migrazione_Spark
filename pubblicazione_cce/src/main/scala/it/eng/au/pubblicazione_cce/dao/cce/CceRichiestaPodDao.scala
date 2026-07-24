package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.dao.HiveDao
import it.eng.au.pubblicazione_cce.model.cce.CceRichiestaPodModel
import it.eng.au.pubblicazione_cce.schema.cce.CceRichiestaPodSchema
import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceRichiestaPodDao extends HiveDao[CceRichiestaPodModel] {
  override val tableName: String = Environment.getCceRichiestaPodTableName
  override val columns: List[String] = CceRichiestaPodSchema.getValues

  //  def read(processo: String = null, ruolo: String = null, tipo: String = null): Dataset[CceRichiestaPodModel] = {
  //    var ds = super.read()
  //    if (processo != null) {
  //      ds = ds.where(col(CceRichiestaPodSchema.t_processo) === processo)
  //    }
  //    if (ruolo != null) {
  //      ds = ds.where(col(CceRichiestaPodSchema.t_ruolo) === ruolo)
  //    }
  //    if (tipo != null) {
  //      ds = ds.where(col(CceRichiestaPodSchema.t_tipo_amm) === tipo)
  //    }
  //    ds
  //  }

}
