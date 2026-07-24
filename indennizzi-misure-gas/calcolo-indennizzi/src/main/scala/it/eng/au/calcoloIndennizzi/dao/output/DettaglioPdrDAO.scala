package it.eng.au.calcoloIndennizzi.dao.output

import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.schema.DettaglioPdrSchema

class DettaglioPdrDAO extends OutputDAO {
  val tableName: String = Properties.getDettaglioPdrTableName
  val parquetPath: String = Properties.getDettaglioPdrPath
  val columns: List[String] = DettaglioPdrSchema.getValues
  val partitionColumns: List[String] = List(DettaglioPdrSchema.annomese, DettaglioPdrSchema.executionid)
}