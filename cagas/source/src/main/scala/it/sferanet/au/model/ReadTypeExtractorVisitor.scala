package it.sferanet.au.model

import it.sferanet.au.controller.visitor.IFlowWithReturnVisitor
import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._


class ReadTypeExtractorVisitor extends IFlowWithReturnVisitor[String] {
  override def visit(model: Tal): String = {
    null
  }

  override def visit(model: Tas): String = {
    null
  }

  override def visit(model: Tml): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: Rgl): String = {
    null
  }

  override def visit(model: Rml): String = {
    null
  }

  override def visit(model: Tav): String = {
    null
  }

  override def visit(model: Tgl): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: Rmv): String = {
    null
  }

  override def visit(model: Rsl): String = {
    null
  }

  override def visit(model: Sw1): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: Tmv): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: A01): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: A40): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: Im1Pre): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: Im1Post): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: Sm1): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: A01R): String = {
    null
  }

  override def visit(model: A02R): String = {
    null
  }

  override def visit(model: A40R): String = {
    null
  }

  override def visit(model: AD2): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: AD2R): String = {
    null
  }

  override def visit(model: AD3): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: AD3R): String = {
    null
  }

  override def visit(model: AD4R): String = {
    null
  }

  override def visit(model: AD5R): String = {
    null
  }

  override def visit(model: AD4): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: AD5): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: FDD): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: FUI): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: IgmgPost): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: IgmgPre): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: IgmrPost): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: IgmrPre): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: M01r): String = {
    null
  }

  override def visit(model: R01r): String = {
    null
  }

  override def visit(model: R40): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: R40r): String = {
    null
  }

  override def visit(model: S02): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: M01): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: Swg1): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: V02): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: A02): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: R01): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: V01): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: S02R): String = {
    null
  }

  override def visit(model: S40R): String = {
    null
  }

  override def visit(model: S40): String = {
    if (model.readType.isDefined) model.readType.get.toString else null
  }

  override def visit(model: V01R): String = {
    null
  }

  override def visit(model: V02R): String = {
    null
  }

  override def visit(model: SM1R): String = {
    null
  }
}
