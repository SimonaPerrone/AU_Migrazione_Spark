package it.eng.au.pubblicazionePcg.model

case class DailyConsumptionModel(
                                  pdr: String = null,
                                  pivait: String = null,
                                  pivaudd: String = null,
                                  pivardb: String = null,
                                  pivaudb: String = null,
                                  codremi: String = null,
                                  idregclim: String = null,
                                  codprofstd: String = null,
                                  treatment: String = null,
                                  date: String = null,
                                  value: Double = 0.0,
                                  tipocliente: String = null,
                                  unitmisprel: String = null,
                                  annomese: String = null,
                                  executionid: String = null
                    )
