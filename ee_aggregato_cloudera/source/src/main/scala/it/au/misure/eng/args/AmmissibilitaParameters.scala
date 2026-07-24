package it.au.misure.eng.args

case class AmmissibilitaParameters(
                       g: String = "",
                       year: String = "",
                       month: String = "",
                       day: String = "",
                       isSmis: Boolean = false
                     ) extends Serializable
