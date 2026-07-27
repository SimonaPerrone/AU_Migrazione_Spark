package it.eng.au.aggiustamentoGas.model.agg

import java.sql.Timestamp

class DailyConsumption (
                          val pdr: String,
                          val date: Timestamp,
                          val value: Option[Double],
                          val pprof: Option[Double],
                          val coefficient: Option[Double],
                          val ca: Option[Double] = None,
                          val idRegClim: Option[Int] = None,
                          val codProfStd: Option[String]= None,
                          val segnante: Option[String],
                          val idFormula: Int,
                          val errorCode:Int,
                          val pivaDistr: Option[String],
                          val pivaUdd: Option[String],
                          val pivaUdb: Option[String],
                          val pivaIt: Option[String],
                          val pivaRdb: Option[String],
                          val dtg: Option[String],
                          val codRemi: Option[String],
                          val tipoCliente: Option[String],
                          val unitMisPrel: Option[String],
                          val annoMese: Option[String],
                          val session: String,
                          val treatment: Option[String],
                          val causale: Option[String],
                          val isValid: Boolean,
                          val leftMeasureLocalFile: Option[String],
                          val rightMeasureLocalFile: Option[String],
                          var forceExclusion: Boolean = false,
                          val tCodIstat: Option[String],
                          val classeMisuratore: Option[String],
                          val valueNotSterilized: Option[Double],
                          val valueF3: Option[Double] = None,
                          val startDateF2: Option[String] = None,
                          val endDateF2: Option[String] = None
                       ) extends Product with Serializable {
  override def productElement(n: Int): Any = n match {
    case 0 => pdr
    case 1 => date
    case 2 => value
    case 3 => pprof
    case 4 => coefficient
    case 5 => ca
    case 6 => idRegClim
    case 7 => codProfStd
    case 8 => segnante
    case 9 => idFormula
    case 10 => errorCode
    case 11 => pivaDistr
    case 12 => pivaUdd
    case 13 => pivaUdb
    case 14 => pivaIt
    case 15 => pivaRdb
    case 16 => dtg
    case 17 => codRemi
    case 18 => tipoCliente
    case 19 => unitMisPrel
    case 20 => annoMese
    case 21 => session
    case 22 => treatment
    case 23 => causale
    case 24 => isValid
    case 25 => leftMeasureLocalFile
    case 26 => rightMeasureLocalFile
    case 27 => forceExclusion
    case 28 => tCodIstat
    case 29 => classeMisuratore
    case 30 => valueNotSterilized
    case 31 => valueF3
    case 32 => startDateF2
    case 33 => endDateF2
  }

  override def productArity: Int = 32

  override def canEqual(that: Any): Boolean = that.isInstanceOf[DailyConsumption]
}
