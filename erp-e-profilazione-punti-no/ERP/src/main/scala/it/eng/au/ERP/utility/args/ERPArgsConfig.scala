package it.eng.au.ERP.utility.args

case class ERPArgsConfig(
                          flow: Option[String] = None,
//                          annomese: Option[Int] = None,
//                          singola_piva_distributore: Option[String] = None,
                          pathToProperties: String = null,
                          path_esclusione_pod: String = null,
                          dati: Option[String] = None
//                          ,area : Option[String] = None
                        ){
override def toString: String = {
  s"ERPArgsConfig=[" +
    s"flow=$flow" +
//  s"annomese=$annomese" +
//  s",singola_piva_distributore=$singola_piva_distributore" +
  s",pathToProperties=$pathToProperties" +
    s",path_esclusione_pod=$path_esclusione_pod" +
    s",dati=$dati" +
//    s",area=$area" +
  s"]"
  }
//  def yearMonth: (Int, Int) = {
//    val value = annomese.getOrElse(
//      throw new IllegalStateException("annomese is not defined, cannot extract year and month.")
//    )
//    val year = value / 100
//    val month = value % 100
//    (year, month)
//  }
  }
