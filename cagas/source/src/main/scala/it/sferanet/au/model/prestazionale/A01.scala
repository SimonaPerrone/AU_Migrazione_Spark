package it.sferanet.au.model.prestazionale

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.Flow.ServiceName
import it.sferanet.au.model.flowTypes.Prestazionale

import java.util.Date

case class A01(service: String, //nomeFlusso
               pdr: String, //cod_prd
               override val readType: Option[Char], //tipoMisura
               date: Option[Date], //data
               pivaDistr: Option[String], //pivaDistr
               pivaUtente: Option[String], //pivaUtente
               override val outcome: Option[Char], //esitoVal
               measure: Option[Double], //segnMis
               converted: Option[Double], //segnConv
               serialNumberMis: Option[String], //matricola misuratore
               serialNumberConv: Option[String], //matricola convertitore
               collected: Option[String],
               local_file: Option[String],
               d_caricamento: Option[Date],
               override val ammissibilita: Option[String],
               isNewRoute: Boolean
              ) extends Prestazionale {

  override def accept(visitor: IFlowVisitor): Unit = {
    visitor.visit(this)
  }

  override def accept[TReturnValue](visitor: IFlowWithReturnVisitor[TReturnValue]): TReturnValue = {
    visitor.visit(this)
  }
}

object A01 {
  val serviceName: String = getClass.getSimpleNameUpperCase
}
