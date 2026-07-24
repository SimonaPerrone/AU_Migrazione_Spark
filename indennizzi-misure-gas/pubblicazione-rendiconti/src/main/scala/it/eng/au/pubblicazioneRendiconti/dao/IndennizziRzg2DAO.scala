package it.eng.au.pubblicazioneRendiconti.dao

import it.eng.au.indennizziMisureGasCommon.schema.IndennizziRzg2Schema
import it.eng.au.pubblicazioneRendiconti.dao.`trait`.DAO
import it.eng.au.pubblicazioneRendiconti.utility.properties.Properties

class IndennizziRzg2DAO extends DAO {
  override val tableName: String = Properties.getCigIndennizziRzg2TableName
  override val columns: List[String] = IndennizziRzg2Schema.getValues
  override val partitionColumn: String = IndennizziRzg2Schema.executionid.toString
}
