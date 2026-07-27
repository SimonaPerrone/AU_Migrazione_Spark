package it.sferanet.au.model

import java.util.Date

case class Weights(
                    date: Date,
                    pprofk: Double,
                    pprof_nk: Double,
                    id_reg_clim: Option[Int],
                    prof: String,
                    wkr: Double) {

}