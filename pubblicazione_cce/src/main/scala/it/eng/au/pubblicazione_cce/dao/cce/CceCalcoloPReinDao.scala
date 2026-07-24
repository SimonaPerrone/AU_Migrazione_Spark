package it.eng.au.pubblicazione_cce.dao.cce

import it.eng.au.pubblicazione_cce.utility.environment.Environment

class CceCalcoloPReinDao extends CceCalcoloDao {
  override val tableName: String = Environment.getCceCalcoloPReinTableName
}
