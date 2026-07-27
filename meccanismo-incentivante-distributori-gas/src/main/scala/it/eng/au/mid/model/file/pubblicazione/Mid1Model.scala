package it.eng.au.mid.model.file.pubblicazione

import it.eng.au.mid.schema.file.pubblicazione.Mid1Schema

/**
 * Contenuto file CSV MID1
 */
case class Mid1Model(
                      PIVA_DISTR: String,
                      PIVA_UDD: String,
                      PDR: String,
                      ANNOMESE: String,
                      N: Int,
                      COD_REMI: String,
                      GDM: String,
                      ALPHA: Int
                    ) {

  override def toString: String = toString(sep = ",")

  def toString(sep: String): String = {
    Seq(PIVA_DISTR, PIVA_UDD, PDR, ANNOMESE, N, COD_REMI, GDM, ALPHA).mkString(sep)
  }
}

object Mid1Model {
  def header(sep: String = ","): String = {
    Mid1Schema.getValues.mkString(sep)
  }
}
