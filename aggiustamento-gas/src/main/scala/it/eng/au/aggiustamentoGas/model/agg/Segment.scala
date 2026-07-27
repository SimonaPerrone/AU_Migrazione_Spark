package it.eng.au.aggiustamentoGas.model.agg

import java.sql.Timestamp

case class Segment(
                    pdr: String,

                    startService: String,
                    startDate: Timestamp,
                    startMeasure: Option[Double],
                    startConverted: Option[Double],
                    startSerialNumberMis: Option[String],
                    startSerialNumberConv: Option[String],
                    startLocalFile: Option[String],
                    startPivaDistr: Option[String],
                    startTreatment: Option[String],
                    startTCodProfilo: Option[String],
                    startIdRegioneClimatica: Option[Int],
                    startTPreConv: Option[String],
                    startGruppoMisInt: Option[String],
                    startCoeff: Option[Double],
                    startSegnante: Option[String],
                    startSegnanteForcingFlag: Option[String],

                    endService: String,
                    endDate: Timestamp,
                    endMeasure: Option[Double],
                    endConverted: Option[Double],
                    endSerialNumberMis: Option[String],
                    endSerialNumberConv: Option[String],
                    endLocalFile: Option[String],
                    endPivaDistr: Option[String],
                    endTreatment: Option[String],
                    endTCodProfilo: Option[String],
                    endIdRegioneClimatica: Option[Int],
                    endTPreConv: Option[String],
                    endGruppoMisInt: Option[String],
                    endCoeff: Option[Double],
                    endSegnante: Option[String],
                    endSegnanteForcingFlag: Option[String]
                  )
//  extends Product with Serializable {
//  override def productElement(n: Int): Any = n match {
//    case 0 => pdr
//    case 1 => startService
//    case 2 => startDate
//    case 3 => startMeasure
//    case 4 => startConverted
//    case 5 => startSerialNumberMis
//    case 6 => startSerialNumberConv
//    case 7 => startLocalFile
//    case 8 => startPivaDistr
//    case 9 => startTreatment
//    case 10 => startTCodProfilo
//    case 11 => startIdRegioneClimatica
//    case 12 => startTPreConv
//    case 13 => startGruppoMisInt
//    case 14 => startCoeff
//    case 15 => startSegnante
//    case 16 => startSegnanteForcingFlag
//    case 17 => endService
//    case 18 => endDate
//    case 19 => endMeasure
//    case 20 => endConverted
//    case 21 => endSerialNumberMis
//    case 22 => endSerialNumberConv
//    case 23 => endLocalFile
//    case 24 => endPivaDistr
//    case 25 => endTreatment
//    case 26 => endTCodProfilo
//    case 27 => endIdRegioneClimatica
//    case 28 => endTPreConv
//    case 29 => endGruppoMisInt
//    case 30 => endCoeff
//    case 31 => endSegnante
//    case 32 => endSegnanteForcingFlag
//  }
//
//  override def productArity: Int = 33
//
//  override def canEqual(that: Any): Boolean = that.isInstanceOf[Segment]
//}