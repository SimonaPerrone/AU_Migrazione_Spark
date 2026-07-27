package it.eng.au.sgsFlussoStoricoGas.model

case class PubListValidatedModel(
                                  pivaUtente: String = null,
                                  outputDir: String = null,
                                  xmlFileName: String = null,
                                  codPdrs: String = null,
                                  zipFilePath: String = null,
                                  zipFileName: String = null,
                                  index: Int = 0,
                                  validationResult: String = null
                                )
