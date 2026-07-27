package it.sferanet.au.model.periodico

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.Flow.ServiceName
import it.sferanet.au.model.flowTypes.Periodico

import java.util.Date

case class Tml(service: String, //nomeFlusso
               pdr: String, //cod_prd
               date: Option[Date], //data
               pivaDistr: Option[String], //pivaDistr
               pivaUtente: Option[String], //pivaUtente
               override val readType: Option[Char], //tipoMisura
               override val isValid: Option[String], //valDato
               measure: Option[Double], //letTotPrel
               converted: Option[Double], //letTotConv
               serialNumberMis: Option[String], //matricola misuratore
               serialNumberConv: Option[String], //matricola convertitore
               local_file: Option[String],
               d_caricamento: Option[Date],
               override val ammissibilita: Option[String],
               isNewRoute: Boolean
              ) extends Periodico {

  override def accept(visitor: IFlowVisitor): Unit = {
    visitor.visit(this)
  }

  override def accept[TReturnValue](visitor: IFlowWithReturnVisitor[TReturnValue]): TReturnValue = {
    visitor.visit(this)
  }
}

object Tml {
  val serviceName: String = getClass.getSimpleNameUpperCase
}