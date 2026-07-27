package it.sferanet.au.controller.visitor

import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._


trait IFlowVisitor extends Serializable {
  // AutoLettura
  def visit(model: Tal)

  def visit(model: Tas)

  def visit(model: Tml)

  // periodico
  def visit(model: Rgl)

  def visit(model: Rml)

  def visit(model: Tav)

  def visit(model: Tgl)

  //prestazionale
  def visit(model: Rmv)

  def visit(model: Rsl)

  def visit(model: Sw1)

  def visit(model: Tmv)

  def visit(model:M01)

  def visit(model:V02)

  def visit(model: A01)

  def visit(model: A40)

  def visit(model: Im1Pre)

  def visit(model: Im1Post)

  def visit(model: Sm1)

  def visit(model: A01R)

  def visit(model: A02R)

  def visit(model: A40R)

  def visit(model: AD2)

  def visit(model: AD2R)

  def visit(model: AD3)

  def visit(model: AD3R)

  def visit(model: AD4R)

  def visit(model: AD5R)

  def visit(model: AD4)

  def visit(model: AD5)

  def visit(model: FDD)

  def visit(model: FUI)

  //NUOVO tecnico
  def visit(model: IgmgPost)

  def visit(model: IgmgPre)

  def visit(model: IgmrPost)

  def visit(model: IgmrPre)

  def visit(model: M01r)

  def visit(model: R01r)

  def visit(model: R40)

  def visit(model: R40r)

  def visit(model: S02)

  def visit(model: Swg1)

  def visit(model: A02)

  def visit(model: R01)

  def visit(model: V01)

  def visit(model: S02R)

  def visit(model: S40R)

  def visit(model: S40)

  def visit(model: V01R)

  def visit(model: V02R)

  def visit(model: SM1R)

}
