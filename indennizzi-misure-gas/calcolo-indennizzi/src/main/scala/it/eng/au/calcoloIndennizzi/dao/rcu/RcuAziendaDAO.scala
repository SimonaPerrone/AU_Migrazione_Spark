package it.eng.au.calcoloIndennizzi.dao.rcu

import it.eng.au.calcoloIndennizzi.dao.DAO
import it.eng.au.calcoloIndennizzi.schema.rcu.RcuAziendaSchema
import it.eng.au.calcoloIndennizzi.utility.Properties

class RcuAziendaDAO extends DAO {
  val parquetPath: String = Properties.getRcuAziendaPath
  val columns: List[String] = RcuAziendaSchema.getValues
}
