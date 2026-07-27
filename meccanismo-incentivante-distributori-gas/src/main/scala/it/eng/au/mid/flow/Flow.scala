package it.eng.au.mid.flow

trait Flow {

  val flowName: String

  def run(): Unit

}
