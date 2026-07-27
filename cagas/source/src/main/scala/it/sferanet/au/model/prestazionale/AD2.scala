package it.sferanet.au.model.prestazionale

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.Flow.ServiceName
import it.sferanet.au.model.flowTypes.Prestazionale

import java.util.Date

case class AD2(service: String,
               pdr: String,
               raccolta: Option[String],
               override val readType: Option[Char],
               date: Option[Date],
               pivaDistr: Option[String],
               pivaUtente: Option[String],
               measure: Option[Double],
               converted: Option[Double],
               serialNumberMis: Option[String],
               serialNumberConv: Option[String],
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

object AD2 {
  val serviceName: String = getClass.getSimpleNameUpperCase
}





