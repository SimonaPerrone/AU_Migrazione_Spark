package it.eng.au.portale_consumi_ee.common.utility.args

trait Args [T]{
  def parse(args: Array[String]): T
}
