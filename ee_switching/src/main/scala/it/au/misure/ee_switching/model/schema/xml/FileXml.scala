package it.au.misure.ee_switching.model.schema.xml

import java.io.File

case class FileXml(
                  chunkName: String = null,
                  file: File = null,
                  podsList: List[String] = null,
                  errorXSD: Boolean = false,
                  errorListXSD: String = null,
                  outputZipPath: String = null
                  )
