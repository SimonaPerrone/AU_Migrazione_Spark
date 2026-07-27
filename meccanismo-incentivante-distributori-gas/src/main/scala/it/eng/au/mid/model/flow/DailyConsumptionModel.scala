package it.eng.au.mid.model.flow

import java.sql.Timestamp

case class DailyConsumptionModel(
                                  pdr: String = null,
                                  date: Timestamp = null,
                                  annomese: String = null,
                                  pivadistr: String = null,
                                  pivaudd: String = null,
                                  codremi: String = null,
                                  classemisuratore: String = null,
                                  treatment: String = null,
                                  session: String = null,
                                  executionid: java.lang.Long = null
                                )
