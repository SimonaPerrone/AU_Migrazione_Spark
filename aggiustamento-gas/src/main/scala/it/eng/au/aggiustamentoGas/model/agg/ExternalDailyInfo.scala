package it.eng.au.aggiustamentoGas.model.agg

import it.eng.au.aggiustamentoGas.model.rcugas._
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.joda.time.DateTime

case class ExternalDailyInfo(
                              rcuGasMassivoPList: Option[Iterable[RcuGasMassivoP]] = None,
                              rcuGasConnessioniDistr2List: Option[Iterable[RcuGasConnessioniDistr2]] = None,
                              rcuGasSospensioniList: Option[Iterable[RcuGasSuspendedPdr]] = None,
                              rcuGasTechList: Option[Iterable[RcuGasTech]] = None,
                              rcuGasVarConvertitoreList: Option[Iterable[RcuGasVarConvertitore]] = None,
                              rcuGasVarPrelAnnuoList: Option[Iterable[RcuGasVarPrelAnnuoP]] = None,
                              rcuGasVarProfiloList: Option[Iterable[RcuGasVarProfiloP]] = None

                            ) {
  def findRcuGasMassivoP(date: DateTime): Option[RcuGasMassivoP] = {
    rcuGasMassivoPList.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.startDate, rc.endDate)
    )
  }

  def getRcuGasMassivoPWithMaxDataFineForn: Option[RcuGasMassivoP] = {
    if(rcuGasMassivoPList.isDefined && rcuGasMassivoPList.get.nonEmpty){
      Option(rcuGasMassivoPList.get.maxBy(_.endDate)(new Ordering[DateTime] {
        override def compare(x: DateTime, y: DateTime): Int = x.compareTo(y)
      }))
    }
    else None
  }

  def findRcuGasConnessioniDistr2(date: DateTime): Option[RcuGasConnessioniDistr2] = {
    rcuGasConnessioniDistr2List.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.dataInizioConn, rc.dataFineConn)
    )
  }

  def findRcuGasSospensioni(date: DateTime): Option[RcuGasSuspendedPdr] = {
    rcuGasSospensioniList.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.dataIniSosp, rc.dataFineSosp)
    )
  }

  def findRcuGasTech(date: DateTime): Option[RcuGasTech] = {
    rcuGasTechList.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.startDateTech, rc.endDateTech)
    )
  }

  def findRcuGasVarConvertitore(date: DateTime): Option[RcuGasVarConvertitore] = {
    rcuGasVarConvertitoreList.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.startDateConv, rc.endDateConv)
    )
  }

  def findRcuGasVarProfiloWithSuspended(date: DateTime): Option[RcuGasVarProfiloP] = {
    rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.dataInizio, rc.dataFine))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(1).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(2).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(3).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(4).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(5).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(6).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(7).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(8).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(9).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(10).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(11).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(12).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(13).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(14).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(15).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(16).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(17).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(18).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(19).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(20).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(21).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(22).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(23).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(24).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(25).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(26).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(27).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(28).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(29).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(30).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(31).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(32).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(33).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(34).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(35).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(36).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(37).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(38).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(39).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(40).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(41).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(42).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(43).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(44).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(45).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(46).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(47).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(48).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(49).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(50).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(51).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(52).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(53).dayOfMonth.withMaximumValue)))
      .orElse(rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
        DateUtility.isBetween(date, rc.dataInizio, rc.dataFine.plusMonths(54).dayOfMonth.withMaximumValue)))
  }

  def findRcuGasVarProfilo(date: DateTime): Option[RcuGasVarProfiloP] = {
    rcuGasVarProfiloList.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.dataInizio, rc.dataFine)
    )
  }

  def findRcuGasVarPrelAnnuo(date: DateTime): Option[RcuGasVarPrelAnnuoP] = {
    rcuGasVarPrelAnnuoList.getOrElse(Iterable()).find(rc =>
      DateUtility.isBetween(date, rc.dataInizio, rc.dataFine)
    )
  }
}
