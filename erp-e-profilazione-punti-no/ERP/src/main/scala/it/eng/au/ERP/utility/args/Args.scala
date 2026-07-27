package it.eng.au.ERP.utility.args

trait Args [T]{
  def parse(args: Array[String]): T
}
