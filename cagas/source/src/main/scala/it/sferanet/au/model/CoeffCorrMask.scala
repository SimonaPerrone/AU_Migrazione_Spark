package it.sferanet.au.model

case class CoeffCorrMask(
                              pdr: String,
                              date: String,
                              service: String,
                              coeff_corr: Option[Double]
                              )
