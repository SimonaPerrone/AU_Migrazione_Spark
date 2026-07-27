package it.sferanet.au.model.autolettura

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.Flow.ServiceName
import it.sferanet.au.model.flowTypes.Autolettura

import java.util.Date

case class Tal(service: String, //nomeFlusso
               pdr: String, //cod_prd
               date: Option[Date], //data
               pivaDistr: Option[String], //pivaDistr
               pivaUtente: Option[String], //pivaUtente
               override val outcome: Option[Char], //esitoVal
               measure: Option[Double], //letTotPrel
               converted: Option[Double], //letTotConv
               serialNumberMis: Option[String], //matricola misuratore
               serialNumberConv: Option[String], //matricola convertitore
               local_file: Option[String],
               d_caricamento: Option[Date],
               override val ammissibilita: Option[String],
               isNewRoute: Boolean
              ) extends Autolettura {

  override def accept(visitor: IFlowVisitor): Unit = {
    visitor.visit(this)
  }

  override def accept[TReturnValue](visitor: IFlowWithReturnVisitor[TReturnValue]): TReturnValue = {
    visitor.visit(this)
  }
}

object Tal {
  val serviceName: String = getClass.getSimpleNameUpperCase
}
