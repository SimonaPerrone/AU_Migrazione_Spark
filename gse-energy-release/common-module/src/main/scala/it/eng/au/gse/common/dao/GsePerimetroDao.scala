package it.eng.au.gse.common.dao

import it.eng.au.gse.common.schema.gse.GsePerimetroSchema
import it.eng.au.gse.common.utility.Properties

class GsePerimetroDao extends Dao {
  override val tableName: String = Properties.gsePerimetroTableName
  override val columns: List[String] = GsePerimetroSchema.getValues
}

object GsePerimetroDao {
  val t_cliente = "t_cliente"
  val t_anno = "t_anno"
}
