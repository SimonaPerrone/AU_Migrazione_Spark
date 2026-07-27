package it.eng.au.mid.model.file.calcolo

case class EsclusioniModel(
                            pdr: String,
                            annomese: String
                          ) {
  override def toString: String = s"pdr: $pdr, annomese: $annomese"
}
