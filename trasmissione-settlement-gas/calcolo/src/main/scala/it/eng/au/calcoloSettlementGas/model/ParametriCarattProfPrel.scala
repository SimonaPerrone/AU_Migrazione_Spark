package it.eng.au.calcoloSettlementGas.model

case class ParametriCarattProfPrel (
                                   b1prof: Double,
                                   b2prof: Double,
                                   b3prof: Double,
                                   b4prof: Double,
                                   categoriaUso: String,
                                   zonaClimatica: Option[String] = None,
                                   ClassePrelievo: String,
                                   prof: String
                                   )
