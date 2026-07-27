package it.eng.au.args

case class AmmissibilitaParameters(
                       g: String = "",
                       year: String = "",
                       month: String = "",
                       day: String = "",
                       isSmis: Boolean = false
                     ) extends Serializable
