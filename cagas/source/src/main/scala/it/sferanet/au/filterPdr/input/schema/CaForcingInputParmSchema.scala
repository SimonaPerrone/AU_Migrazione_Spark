package it.sferanet.au.filterPdr.input.schema


object CaForcingInputParmSchema extends Enumeration {
  type CaForcingInputParmSchema = Value
  val pdr = Value("pdr")
  val ca = Value("ca")
  val codPrel = Value("cod_prel")
  val catUso = Value("cat_uso")
  val zonClimatica = Value("zona_climatica")
  val classePrelievo = Value("classe_prelievo")
  val trattamento = Value("trattamento")
}
