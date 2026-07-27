package it.eng.au.aggiustamentoGas.model.agg

import java.sql.Timestamp

class ValidatedFlow(
                          val service: String,
                          val pdr: String,
                          val date: Option[Timestamp],
                          val measure: Option[Double],
                          val converted: Option[Double],
                          val serialNumberMis: Option[String],
                          val serialNumberConv: Option[String],
                          val localFile: Option[String],
                          val dataCaricamento: Option[Timestamp],
                          val isValid: Option[String],
                          val outcome: Option[String],
                          val readType: Option[String],
                          val motivation: Option[Int],
                          val treatment: String,
                          val codProfilo: Option[String],
                          val nCoeffCor: Option[Double],
                          val gruppoMisInt: Option[String],
                          val tPreConv: Option[String],
                          val calcCoeff: Option[Double],
                          val idRegioneClimatica: Option[String],
                          val isCorrected: Boolean,
                          val segnanteForcingFlag: Option[String],
                          val cauIntMis: Option[Int],
                          val cauIntCorr: Option[Int],
                          val classeMisuratore: Option[String]
                        ) extends Product with Serializable {
  override def productElement(n: Int): Any = n match {
    case 0 => service
    case 1 => pdr
    case 2 => date
    case 3 => measure
    case 4 => converted
    case 5 => serialNumberMis
    case 6 => serialNumberConv
    case 7 => localFile
    case 8 => dataCaricamento
    case 9 => isValid
    case 10 => outcome
    case 11 => readType
    case 12 => motivation
    case 13 => treatment
    case 14 => codProfilo
    case 15 => nCoeffCor
    case 16 => gruppoMisInt
    case 17 => tPreConv
    case 18 => calcCoeff
    case 19 => idRegioneClimatica
    case 20 => isCorrected
    case 21 => segnanteForcingFlag
    case 22 => cauIntMis
    case 23 => cauIntCorr
    case 24 => classeMisuratore
  }

  override def productArity: Int = 25

  override def canEqual(that: Any): Boolean = that.isInstanceOf[ValidatedFlow]
}
