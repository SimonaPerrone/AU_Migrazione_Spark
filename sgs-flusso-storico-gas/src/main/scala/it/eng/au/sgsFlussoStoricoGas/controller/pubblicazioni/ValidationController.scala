package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni

import it.eng.au.sgsFlussoStoricoGas.model.{PubListModel, PubListValidatedModel}

import javax.xml.XMLConstants
import javax.xml.validation.{SchemaFactory, Validator}
import java.io.File
import javax.xml.transform.stream.StreamSource
import org.xml.sax.SAXException

import java.nio.file.{Files, Paths}

class ValidationController {

  def createValidator(xsdPath: String): Validator = {
    val schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
    val schema = schemaFactory.newSchema(new File(xsdPath))
    schema.newValidator()
  }

  /**
   * Valida una lista di XML rispetto a uno schema XSD e restituisce una lista di tuple con il risultato.
   *
   * @param validator Validator per la validazione XML.
   * @param data Lista di tuple contenenti informazioni sugli XML.
   * @return Lista di tuple originali con il risultato della validazione ("OK" , "KO", "PATH_NOT_FOUND") aggiunto.
   *         L'ultimo risultato verrà filtrato per non apparire nell'output
   */
  def validateXml(validator: Validator, data: List[PubListModel]):
  List[PubListValidatedModel] = {

    data.map { dataItem =>
      val fullPath = Paths.get(dataItem.outputDir, dataItem.xmlFileName).toString

      val validationResult =
        if (!Files.exists(Paths.get(dataItem.outputDir))) {
          println(s"Path non trovato: ${dataItem.outputDir}")
          "PATH_NOT_FOUND"
        } else {
          try {
            validator.validate(new StreamSource(new File(fullPath)))
            "OK"
          } catch {
            case e: SAXException =>
              println(s"Errore di validazione: ${e.getMessage}")
              "KO"
            case ex: Exception =>
              throw new RuntimeException(s"Errore generico durante la validazione del file: $fullPath", ex)
          }
        }

      // Crea una nuova tupla aggiungendo il risultato della validazione
      PubListValidatedModel(dataItem.pivaUtente, dataItem.outputDir, dataItem.xmlFileName, dataItem.codPdrs, dataItem.zipFilePath, dataItem.zipFileName, dataItem.index, validationResult)
    }
  }.filter(_.validationResult != "PATH_NOT_FOUND")
}


