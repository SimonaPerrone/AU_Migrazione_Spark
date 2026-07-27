package it.au.misure.ee_switching.utility

import it.au.misure.ee_switching.model.schema.xml.FileXml
import javax.xml.XMLConstants
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.{Schema, SchemaFactory, Validator}
import org.apache.spark.rdd.RDD
import java.io.FileReader

import it.au.misure.ee_switching.utility.Constants.FUNZIONALI
import org.xml.sax.{ErrorHandler, SAXParseException}

import scala.collection.mutable.ArrayBuffer


object ValidateUtility {

  def validateXmlFiles(xmlRdd: RDD[FileXml], flowName: String): RDD[FileXml] = {

    xmlRdd.mapPartitions(xmlFiles => {

      // TODO: valutare se eliminare il mapPartitions e mettere tutto dentro alla successiva map
      val schemaFilePath: String =
      if (flowName.equals(FUNZIONALI))
        PropertyUtility.getXSDFunzionali // instance of XSD file.
      else
        PropertyUtility.getXSDStorici // instance of XSD file.

      val factory: SchemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
      val schema: Schema = factory.newSchema(new StreamSource(schemaFilePath)) // instance of schema

      xmlFiles.map(xmlFile => {

        val validator: Validator = schema.newValidator() // instance of validator

        val exceptions: ArrayBuffer[SAXParseException] = ArrayBuffer() // Empty list to store errors
        // Create a custom error handler that populates the list when errors occur.
        validator.setErrorHandler(new ErrorHandler() {

          @Override
          def warning(exception: SAXParseException) {
            exceptions.append(exception)
          }

          @Override
          def fatalError(exception: SAXParseException) {
            exceptions.append(exception)
          }

          @Override
          def error(exception: SAXParseException) {
            exceptions.append(exception)
          }
        })

        validator.validate(new StreamSource(new FileReader(xmlFile.file)))
        val messageList = if (exceptions.nonEmpty)
//          exceptions.map(exception => s"Error: ${exception.getMessage}, Line: ${exception.getLineNumber}, Column: ${exception.getColumnNumber}").mkString("; ")
          exceptions.map(exception => s"Line ${exception.getLineNumber}: ${exception.getMessage}").mkString(" - ")
        else
          Constants.OK

        xmlFile.copy(errorXSD = exceptions.nonEmpty, errorListXSD = messageList)
      })
    })
  }

}
