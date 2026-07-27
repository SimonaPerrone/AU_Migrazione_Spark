package it.sferanet.au.model.rettifica

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.Flow.ServiceName
import it.sferanet.au.model.flowTypes.{Prestazionale, Rettifica}

import java.util.Date

case class AD3R(service: String,
                pdr: String,
                override val motivation: Option[Int],
                raccolta: Option[String],
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
               ) extends Rettifica with Prestazionale {

  override def accept(visitor: IFlowVisitor): Unit = {
    visitor.visit(this)
  }

  override def accept[TReturnValue](visitor: IFlowWithReturnVisitor[TReturnValue]): TReturnValue = {
    visitor.visit(this)
  }
}

object AD3R {
  val serviceName: String = getClass.getSimpleNameUpperCase
}





