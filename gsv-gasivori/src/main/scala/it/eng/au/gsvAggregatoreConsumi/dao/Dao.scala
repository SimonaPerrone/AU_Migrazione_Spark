package it.eng.au.gsvAggregatoreConsumi.dao

trait Dao {
  val tablePath: String
  val tableName: String
  val columns: List[String]
}
