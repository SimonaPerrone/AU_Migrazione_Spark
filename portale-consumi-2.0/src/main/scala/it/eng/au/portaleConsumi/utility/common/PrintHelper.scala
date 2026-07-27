package it.eng.au.portaleConsumi.utility.common

object PrintHelper {

  /***
   * Converte in stringa gli elementi all'interno dell'array. Usato per il print di case class complesse
   */
  def attributeToString(attributeList: List[Any]): String = {
    var str = ""
    for ((f, i) <- attributeList.zipWithIndex) {
      str = str + "\n" + f.toString
      if (i+1 < attributeList.length){
        str = str + ","
      }
    }
    str
  }
}
