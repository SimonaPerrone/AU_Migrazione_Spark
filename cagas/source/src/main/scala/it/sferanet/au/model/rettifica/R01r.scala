package it.sferanet.au.model.rettifica

import it.sferanet.au.controller.visitor._
import it.sferanet.au.model.Flow.ServiceName
import it.sferanet.au.model.flowTypes.{Prestazionale, Rettifica}

import java.util.Date

case class R01r(service: String, //nomeFlusso
                pdr: String, //cod_prd
                collected: Option[String], //raccolta
                date: Option[Date], //data
                pivaDistr: Option[String], //pivaDistr
                pivaUtente: Option[String], //pivaUtente
                measure: Option[Double], //letTotPrel
                converted: Option[Double], //letTotConv
                serialNumberMis: Option[String], //matricola misuratore
                serialNumberConv: Option[String], //matricola convertitore
                override val motivation: Option[Int],
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

object R01r {
  val serviceName: String = getClass.getSimpleNameUpperCase
}




