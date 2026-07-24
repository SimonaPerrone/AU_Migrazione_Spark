package it.eng.au.pubblicazione_cce.model.file

case class ZipFileModel(
                         id_richiesta: String,
                         files: List[String], // lista file (nome completo) da aggiungere allo zip
                         filePathRoot: String,
                         fileName: String, // file name zip
                         filePathSubDirectories: String
                       )
