package it.eng.au.cceCalcolo.dao

trait Dao {
  val tablePath: String
  val tableName: String
  val columns: List[String]
}
