package it.sferanet.au.model

case class ForcingMask(
                      pdr: String,
                      date: String,
                      service: String,
                      cau_int_cor: Option[Int]
                      )
