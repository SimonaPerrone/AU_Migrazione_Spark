package it.sferanet.au.controller.visitor

import it.sferanet.au.model.autolettura._
import it.sferanet.au.model.periodico._
import it.sferanet.au.model.prestazionale._
import it.sferanet.au.model.rettifica._


trait IFlowWithReturnVisitor[TReturnValue] extends Serializable {
  // AutoLettura
  def visit(model: Tal): TReturnValue

  def visit(model: Tas): TReturnValue

  def visit(model: Tml): TReturnValue

  // periodico
  def visit(model: Rgl): TReturnValue

  def visit(model: Rml): TReturnValue

  def visit(model: Tav): TReturnValue

  def visit(model: Tgl): TReturnValue

  //prestazionale
  def visit(model: Rmv): TReturnValue

  def visit(model: Rsl): TReturnValue

  def visit(model: Sw1): TReturnValue

  def visit(model: Tmv): TReturnValue

  //NUOVO prestazionale
  def visit(model: M01): TReturnValue

  def visit(model: V02): TReturnValue

  // tecnico
  def visit(model: A01): TReturnValue

  def visit(model: A40): TReturnValue

  def visit(model: Im1Pre): TReturnValue

  def visit(model: Im1Post): TReturnValue

  def visit(model: Sm1): TReturnValue

  def visit(model: A01R): TReturnValue

  def visit(model: A02R): TReturnValue

  def visit(model: A40R): TReturnValue

  def visit(model: AD2): TReturnValue

  def visit(model: AD2R): TReturnValue

  def visit(model: AD3): TReturnValue

  def visit(model: AD3R): TReturnValue

  def visit(model: AD4R): TReturnValue

  def visit(model: AD5R): TReturnValue

  def visit(model: AD4): TReturnValue

  def visit(model: AD5): TReturnValue

  def visit(model: FDD): TReturnValue

  def visit(model: FUI): TReturnValue

  //Nuovo tecnico
  def visit(model: IgmgPost): TReturnValue

  def visit(model: IgmgPre): TReturnValue

  def visit(model: IgmrPost): TReturnValue

  def visit(model: IgmrPre): TReturnValue

  //NUOVO standard
  def visit(model: M01r): TReturnValue

  def visit(model: R01r): TReturnValue

  def visit(model: R40): TReturnValue

  def visit(model: R40r): TReturnValue

  def visit(model: S02): TReturnValue

  def visit(model: Swg1): TReturnValue

  def visit(model: A02): TReturnValue

  def visit(model: R01): TReturnValue

  def visit(model: V01): TReturnValue

  def visit(model: S02R): TReturnValue

  def visit(model: S40R): TReturnValue

  def visit(model: S40): TReturnValue

  def visit(model: V01R): TReturnValue

  def visit(model: V02R): TReturnValue

  def visit(model: SM1R): TReturnValue

}
