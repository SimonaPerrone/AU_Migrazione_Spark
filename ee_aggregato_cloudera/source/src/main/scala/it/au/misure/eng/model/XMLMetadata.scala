package it.au.misure.eng.model

import it.au.misure.eng.args.AmmissibilitaParameters
import org.apache.spark.broadcast.Broadcast

import java.io.File
import java.time.{LocalDate, LocalDateTime}
import javax.xml.validation.Validator

class XMLMetadata(
                   val file: File,
                   val pivaDistributore: String,
                   val pivaUDD: String,
                   val annoMese: String,
                   val flusso: String,
                   val timestamp: String,
                   val progressivo: String,
                   val codDp: String,
                   val sm: String, //smis flow not use this field
                   val codFlusso: String = "",
                   val params: AmmissibilitaParameters = null,
                   val flusso1XSDBroad: Validator = null,
                   val flusso2XSDBroad: Validator = null,
                   val mapFileNames: Broadcast[Map[String, Map[String, Set[String]]]] = null,
                   val mapPivaRcu: Broadcast[Map[String, (LocalDateTime, LocalDateTime)]] = null,
                   val mapCodDPRcuPivaUdd: Broadcast[Map[String, Set[String]]] = null,
                   val alreadyTransmitted: Boolean = false,

                   //fileds for smis flow
                   val flussoSMISXSDBroad: Validator = null,
                   val listPivaRcuDistr: Broadcast[List[String]] = null,
                   val listPivaRcuEmt: Broadcast[List[String]] = null,
                   val idXml: Long = 0,
                   val isPodCompetenceDistr: Boolean = false,
                   val isPodCompetenceUdd: Boolean = false

                 ) extends Product with Serializable {
  override def productElement(n: Int): Any = n match {
    case 0 => file
    case 1 => pivaDistributore
    case 2 => pivaUDD
    case 3 => annoMese
    case 4 => flusso
    case 5 => timestamp
    case 6 => progressivo
    case 7 => codDp
    case 8 => sm
    case 9 => codFlusso
    case 10 => params
    case 11 => flusso1XSDBroad
    case 12 => flusso2XSDBroad
    case 13 => mapFileNames
    case 14 => mapPivaRcu
    case 15 => mapCodDPRcuPivaUdd
    case 16 => alreadyTransmitted
    case 17 => flussoSMISXSDBroad
    case 18 => listPivaRcuDistr
    case 19 => listPivaRcuEmt
    case 20 => idXml
    case 21 => isPodCompetenceDistr
    case 22 => isPodCompetenceUdd
  }

  override def productArity: Int = 24

  override def canEqual(that: Any): Boolean = that.isInstanceOf[XMLMetadata]


  def copy(
            file: File = this.file,
            pivaDistributore: String = this.pivaDistributore,
            pivaUDD: String = this.pivaUDD,
            annoMese: String = this.annoMese,
            flusso: String = this.flusso,
            timestamp: String = this.timestamp,
            progressivo: String = this.progressivo,
            codDp: String = this.codDp,
            sm: String = this.sm,
            codFlusso: String = this.codFlusso,
            params: AmmissibilitaParameters = this.params,
            flusso1XSDBroad: Validator = this.flusso1XSDBroad,
            flusso2XSDBroad: Validator = this.flusso2XSDBroad,
            mapFileNames: Broadcast[Map[String, Map[String, Set[String]]]] = this.mapFileNames,
            mapPivaRcu: Broadcast[Map[String, (LocalDateTime, LocalDateTime)]] = this.mapPivaRcu,
            mapCodDPRcuPivaUdd: Broadcast[Map[String, Set[String]]] = this.mapCodDPRcuPivaUdd,
            alreadyTransmitted: Boolean = this.alreadyTransmitted,
            flussoSMISXSDBroad: Validator = this.flussoSMISXSDBroad,
            listPivaRcuDistr: Broadcast[List[String]] = this.listPivaRcuDistr,
            listPivaRcuEmt: Broadcast[List[String]] = this.listPivaRcuEmt,
            idXml: Long = this.idXml,
            isPodCompetenceDistr: Boolean = this.isPodCompetenceDistr,
            isPodCompetenceUdd: Boolean = this.isPodCompetenceUdd

          ) = new XMLMetadata(file,
    pivaDistributore,
    pivaUDD,
    annoMese,
    flusso,
    timestamp,
    progressivo,
    codDp,
    sm,
    codFlusso,
    params,
    flusso1XSDBroad,
    flusso2XSDBroad,
    mapFileNames,
    mapPivaRcu,
    mapCodDPRcuPivaUdd,
    alreadyTransmitted,
    flussoSMISXSDBroad,
    listPivaRcuDistr,
    listPivaRcuEmt,
    idXml,
    isPodCompetenceDistr,
    isPodCompetenceUdd)

}

