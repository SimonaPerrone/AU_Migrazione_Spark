package it.au.misure.ingestionMisureGasUnico.model

import java.time.LocalDateTime

import javax.xml.validation.Validator
import org.apache.spark.broadcast.Broadcast


case class ExternalInfo(
                         flusso1XSD: Validator = null
                         , flusso2XSD: Validator = null
                         , flussoIGMGXSD: Validator = null
                         , flussoIGMRXSD: Validator = null
                         , mapFilesName: Broadcast[Map[String,Map[String,Set[String]]]] = null
                         , uDDActivePeriodsMap: Broadcast[Map[String, (LocalDateTime, LocalDateTime)]] = null
                       )
