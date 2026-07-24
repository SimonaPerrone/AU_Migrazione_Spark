package it.au.misure.calcolo_capacita.component.utility.`implicit`


object ListUtility {

  implicit class ListUtility(list: List[String]) {

    def -(colName: String): List[String] = {
      list.filter(_ != colName)
    }
  }

}