package it.au.misure.ee_switching.args

import java.time.LocalDate

import java.sql.Timestamp

case class FlowArgsConfig(
                           flowName: String = null,
                           runOrdinaria: Boolean = true, // se true allora verranno considerate le date del giorno corrente
                           timestamp: Timestamp = null,
                           listaPod: Seq[String] = Seq(),
                           listaDistributori: Seq[String] = Seq(),
                           listaUdd: Seq[String] = Seq(),
                           listaDateSW: Seq[LocalDate] = Seq(),
                           listaDateNA: Seq[LocalDate] = Seq(),
                           listaCoppieDistrUdd: Seq[(String, String)] = Seq()
                       ) {

  override def toString: String = s"FlowArgsConfig=[" +
    s"flowName=$flowName, " +
    s"runOrdinaria=$runOrdinaria, " +
    s"timestamp=$timestamp, " +
    s"listaPod=$listaPod, " +
    s"listaDistributori=$listaDistributori, " +
    s"listaUdd=$listaUdd, " +
    s"listaDateSW=$listaDateSW, " +
    s"listaDateNA=$listaDateNA, " +
    s"listaCoppieDistrUdd=$listaCoppieDistrUdd" +
    s"]"

}
