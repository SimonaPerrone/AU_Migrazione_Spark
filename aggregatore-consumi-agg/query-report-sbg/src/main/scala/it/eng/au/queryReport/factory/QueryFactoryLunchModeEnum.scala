package it.eng.au.queryReport.factory

object QueryFactoryLunchModeEnum extends Enumeration {
  val DETTAGLIOUNICO: QueryFactoryLunchModeEnum.Value = Value("DETTAGLIOUNICO")
  val SOSPESI: QueryFactoryLunchModeEnum.Value = Value("SOSPESI")
  val AGGREGATO: QueryFactoryLunchModeEnum.Value = Value("AGGREGATO")
  val ESCLUSI: QueryFactoryLunchModeEnum.Value = Value("ESCLUSI")
  val INCOERENTI: QueryFactoryLunchModeEnum.Value = Value("INCOERENTI")
  val DETTAGLIOINCOERENTI: QueryFactoryLunchModeEnum.Value = Value("DETTAGLIOINCOERENTI")
  val DELTANEGATIVO: QueryFactoryLunchModeEnum.Value = Value("DN")
  val GIROCONTATORE: QueryFactoryLunchModeEnum.Value = Value("GIRO")

  private val m = this.values.map(v => (v.toString, v)).toMap

  def toValue(s: String): Value = this.m.get(s).orNull

}
