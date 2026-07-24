package it.eng.au.calcoloIndennizzi.dao.measure

import it.eng.au.calcoloIndennizzi.schema.measure.TglSchema
import it.eng.au.calcoloIndennizzi.utility.Properties

class TglDAO extends MeasureDAO {
  val parquetPath: String = ""
  val columns: List[String] = TglSchema.getValues
  val tableName: String = Properties.getTglTableName
}
