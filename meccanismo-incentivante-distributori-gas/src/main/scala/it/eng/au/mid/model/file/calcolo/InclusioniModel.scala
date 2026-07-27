package it.eng.au.mid.model.file.calcolo

case class InclusioniModel(
                            pdr: String,
                            annomese: String,
                            n: java.lang.Integer
                          ) {
  override def toString: String = s"pdr: $pdr, annomese: $annomese, n: $n"
}
