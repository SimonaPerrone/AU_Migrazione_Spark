package it.au.misure.ingestionMisureGasUnico.args

import java.time.LocalDate

case class UnzipArgsConfig(
                            fromDate: Option[LocalDate] = None
                            , toDate: Option[LocalDate] = None
                            , flows: Option[Set[String]] = None
                            , recovery: Boolean = false
                            , fileRecovery: Boolean = false
                            , oldOnly: Boolean = false
                            , standardAndIgmgAndIgmr: Boolean = false
                            , standardOnly: Boolean = false
                            , igmgOnly: Boolean = false
                            , standardAndIgmg: Boolean = false
                            , igmgAndIgmr: Boolean = false
                          ) {
  override def toString: String = s"UnzipArgsConfig=[" +
    s"fromDate=$fromDate" +
    s", toDate=$toDate" +
    s", flows=$flows" +
    s", recovery=$recovery" +
    s", fileRecovery=$fileRecovery" +
    s", oldOnly=$oldOnly" +
    s", standardAndIgmgAndIgmr=$standardAndIgmgAndIgmr" +
    s", standardOnly=$standardOnly" +
    s", igmgOnly=$igmgOnly" +
    s", standardAndIgmg=$standardAndIgmg" +
    s", IgmgAndIgmr=$igmgAndIgmr" +
    s"]"
}
