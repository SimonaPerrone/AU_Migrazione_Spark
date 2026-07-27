package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.filter.inclusion.InclusionFilterController
import it.eng.au.aggiustamentoGas.model.agg.{ExternalDailyInfo, FlowWithInfo, MonthTreatment}
import it.eng.au.aggiustamentoGas.model.measure.im1Igmg.{Igmg, IgmgPost, IgmgPre, Im1, Im1Igmg, Im1Post, Im1Pre, Post, Pre}
import it.eng.au.aggiustamentoGas.model.measure.{A01, A02, A40, Ad2, Ad3, Ad4, Ad5, D01, D02, Flow, M01, R01, R40, S02, S40, Sm1, Sm2, Tal, Tas, Tav, V01, V02}
import it.eng.au.aggiustamentoGas.model.rcugas._
import it.eng.au.aggiustamentoGas.utility.constants.Treatment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat

/** Associa alle misure le varie informazioni prese da rcugas */
class JoinInfoController(private val inclusionFilters: List[InclusionFilterController] = List()) extends Serializable {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  /**
   * Estrae le informazioni da rcugas che serviranno successivamente per il calcolo dei consumi.
   * @param measures misure alle quali associare le informazioni
   * @param monthTreatment trattamento mensile per ogni PdR
   * @param rcuGasMassivo info sulla fornitura
   * @param rcuGasConnessioniDistr2 info sul distributore (codice remi e id regione climatica)
   * @param suspendedPdr PdR sospesi
   * @param rcuGasTech classe misuratore per ogni PdR
   * @param rcuGasVarPrelAnnuo prelievo annuo per ogni PdR
   * @param rcuGasVarProfilo utilizzata per estrarre il codice profilo durante il calcolo dei consumi
   * @param startPeriodDate estremo sinistro del periodo di calcolo dei consumi
   * @param endPeriodDate estremo destro del periodo di calcolo dei consumi
   * @return key-value RDD, dove la chiave è l'n_id_pdr e il valore è composto dalle informazioni associate alla misura ([[FlowWithInfo]], per il momento vuote),
   *         e [[ExternalDailyInfo]], contenente le informazioni estratte da rcugas
   */
  def get(measures: RDD[Flow],
          monthTreatment: RDD[MonthTreatment],
          rcuGasMassivo: RDD[RcuGasMassivoP],
          rcuGasConnessioniDistr2: RDD[RcuGasConnessioniDistr2],
          suspendedPdr: RDD[RcuGasSuspendedPdr],
          rcuGasTech: RDD[RcuGasTech],
          rcuGasVarPrelAnnuo: RDD[RcuGasVarPrelAnnuoP],
          rcuGasVarProfilo: RDD[RcuGasVarProfiloP],
          rcuGasVarConvertitore: RDD[RcuGasVarConvertitore],
          startPeriodDate: String,
          endPeriodDate: String): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {

    val groupedMeasures = groupMeasuresAndSort(measures, startPeriodDate, endPeriodDate)
    val measuresWithTreatment = setFlowTreatment(groupedMeasures, monthTreatment)
    //setRcuGasMassivoAndDatiPrelievo must be called exactly here and before setRcuGasConnessioniDistr2 and
    // setSuspendedPdrs since it allows to have all pdrs from rcu even if without measures.
    val measuresWithRcu = setRcuGasMassivoAndDatiPrelievo(measuresWithTreatment.mapValues(f => (f, ExternalDailyInfo())), rcuGasMassivo, rcuGasTech, rcuGasVarConvertitore, rcuGasVarPrelAnnuo, rcuGasVarProfilo)
    val measureWithConnessioniDistr2 = setRcuGasConnessioniDistr2(measuresWithRcu, rcuGasConnessioniDistr2)
    val measureWithSuspension = setSuspendedPdrs(measureWithConnessioniDistr2, rcuGasMassivo, suspendedPdr)
    filterAutoletturaFlow(measureWithSuspension, startPeriodDate, endPeriodDate)

  }

  //Sbg override this, in agg filter also the stimate (readType == 'S')

  /**
   * Rimuove alcune misure come richiesto da AU. Per maggiori dettagli, consultare i documenti tecnici.
   */
  def filterAutoletturaFlow(measureWithSuspension: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))], startDate: String, endDate: String): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {
    measureWithSuspension.mapValues({ case (fList, externalDailyInfo) =>
      (fList.filter(f =>
        f.monthTreatment.isEmpty ||
          (f.monthTreatment.get.treatment == Treatment.G.toString && (f.flow.readType != Some('A') && !List(classOf[Tal], classOf[Tas], classOf[Tav]).exists(_.isInstance(f.flow)))) ||
          (f.monthTreatment.get.treatment != Treatment.G.toString && (f.flow.readType == Some('S') && List(classOf[Im1Igmg], classOf[Im1], classOf[Igmg], classOf[IgmgPre], classOf[IgmgPost], classOf[Im1Post], classOf[Im1Pre], classOf[Pre], classOf[Post], classOf[D01], classOf[D02], classOf[R01], classOf[A40], classOf[R40], classOf[S40], classOf[A01], classOf[A02], classOf[S02], classOf[V01], classOf[M01], classOf[V02], classOf[Sm1], classOf[Sm2], classOf[Ad2], classOf[Ad3], classOf[Ad4], classOf[Ad5]).exists(_.isInstance(f.flow)))) ||
          (f.monthTreatment.get.treatment != Treatment.G.toString && f.flow.readType != Some('S'))
      ),
        externalDailyInfo)
    })
  }

  /**
   *
   * @param measures  measures
   * @param startDate start period date
   * @param endDate   end period date
   * @return measures grouped by pdr only in period with one measure immediately before startDate and one measure after end date, ordered by date
   */
  def groupMeasuresAndSort(measures: RDD[Flow], startDate: String, endDate: String): RDD[(String, List[FlowWithInfo])] = {
    val inputDateFormatter = DateTimeFormat.forPattern("yyyyMM")
    val startDateJoda = inputDateFormatter.parseDateTime(startDate)
    val endDateJoda = inputDateFormatter.parseDateTime(endDate).dayOfMonth().withMaximumValue()

    measures.map(f => FlowWithInfo(flow = f)).keyBy(_.flow.pdr)
      .groupByKey().mapValues(measures => {
      val measuresInPeriod = measures.filter(f => DateUtility.isBetween(f.flow.date.get, startDateJoda, endDateJoda))
      val measuresNotInPeriod = measures.filter(f => !DateUtility.isBetween(f.flow.date.get, startDateJoda, endDateJoda))

      // find the first measure immediately before startDate, if exists
      val measuresNearStartDate = measuresNotInPeriod.filter(f => f.flow.date.get.isBefore(startDateJoda))
        .map(f => (f, Days.daysBetween(f.flow.date.get, startDateJoda).getDays))
      val measureNearStartDate = if (measuresNearStartDate.isEmpty) List() else List(measuresNearStartDate.minBy(_._2)._1)

      // find the first measure immediately after endDate, if exists
      val measuresNearEndDate = measuresNotInPeriod.filter(f => f.flow.date.get.isAfter(endDateJoda))
        .map(f => (f, Days.daysBetween(endDateJoda, f.flow.date.get).getDays))
      val measureNearEndDate = if (measuresNearEndDate.isEmpty) List() else List(measuresNearEndDate.minBy(_._2)._1)

      (measuresInPeriod.toList ++ measureNearStartDate ++ measureNearEndDate).sorted(FlowWithInfo.orderingFlowsByDateTime)
    })
  }

  def setFlowTreatment(measures: RDD[(String, List[FlowWithInfo])], monthTreatment: RDD[MonthTreatment]): RDD[(String, List[FlowWithInfo])] = {
    measures.leftOuterJoin(monthTreatment.keyBy(t => t.pdr).groupByKey().mapValues(treatList => treatList.map(treat => (treat.month, treat)).toMap))
      .mapValues({ case (flowsWithInfo, monthTreatmentMap) => flowsWithInfo.map(f => f.copy(monthTreatment = monthTreatmentMap.flatMap(_.get(f.flow.date.get.toString("yyyyMM"))))) })
  }

  /** Estrae da rcuags alcune informazioni che serviranno per il calcolo dei consumi, e inserisce queste info in [[FlowWithInfo]] e [[ExternalDailyInfo]]. */
  def setRcuGasMassivoAndDatiPrelievo(measures: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))],
                                      rcuGasMassivo: RDD[RcuGasMassivoP],
                                      rcuTech: RDD[RcuGasTech],
                                      rcuGasVarConvertitore: RDD[RcuGasVarConvertitore],
                                      rcuGasVarPrelAnnuo: RDD[RcuGasVarPrelAnnuoP],
                                      rcuGasVarProfilo: RDD[RcuGasVarProfiloP]
                                     ): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {

    val joinMassivoPrelievo = rcuGasMassivo
      .keyBy(_.nIdPdr)
      .groupByKey
      .leftOuterJoin(rcuTech.keyBy(_.nIdPdr).groupByKey())
      .leftOuterJoin(rcuGasVarConvertitore.keyBy(_.nIdPdr).groupByKey())
      .leftOuterJoin(rcuGasVarPrelAnnuo.keyBy(_.nIdPdr).groupByKey())
      .leftOuterJoin(rcuGasVarProfilo.keyBy(_.nIdPdr).groupByKey())

    val joinMassivoPrelievoMapped = joinMassivoPrelievo
      .mapValues {
        case (
          (
            (
              (
                rcuGasMassivoIter,
                rcuGasTechOpt
                ),
              rcuGasVarConvertitoreOpt
              ),
            rcuGasVarPrelAnnuoOpt
            ),
          rcuGasVarProfiloOpt
          ) =>
          (
            rcuGasMassivoIter,
            rcuGasTechOpt.getOrElse(Iterable()),
            rcuGasVarConvertitoreOpt.getOrElse(Iterable()),
            rcuGasVarPrelAnnuoOpt.getOrElse(Iterable()),
            rcuGasVarProfiloOpt.getOrElse(Iterable())
          )
      }
      .values


    val measureJoinedWithMassivo =
      if (inclusionFilters.isEmpty) {
        measures
          .fullOuterJoin(joinMassivoPrelievoMapped.keyBy(_._1.head.tCodicePdr))
          .mapValues {
            case (optMeasures, optRcu) =>
              val (flowWithInfoList, extInfos) =
                optMeasures.getOrElse((List(), ExternalDailyInfo()))

              val (
                rcuGasMassivoList,
                rcuGasTechList,
                rcuGasVarConvertitoreList,
                rcuGasVarPrelAnnuoList,
                rcuGasVarProfiloList
                ) =
                optRcu.getOrElse(
                  (
                    Iterable(),
                    Iterable(),
                    Iterable(),
                    Iterable(),
                    Iterable()
                  )
                )

              (
                (flowWithInfoList, extInfos),
                (
                  rcuGasMassivoList,
                  rcuGasTechList,
                  rcuGasVarConvertitoreList,
                  rcuGasVarPrelAnnuoList,
                  rcuGasVarProfiloList
                )
              )
          }
      }
      else {
        measures
          .leftOuterJoin(joinMassivoPrelievoMapped.keyBy(_._1.head.tCodicePdr))
          .mapValues {
            case (measuresWithExtInfosTpl, optRcu) =>
              val (
                rcuGasMassivoList,
                rcuGasTechList,
                rcuGasVarConvertitoreList,
                rcuGasVarPrelAnnuoList,
                rcuGasVarProfiloList
                ) =
                optRcu.getOrElse(
                  (
                    Iterable(),
                    Iterable(),
                    Iterable(),
                    Iterable(),
                    Iterable()
                  )
                )

              (
                measuresWithExtInfosTpl,
                (
                  rcuGasMassivoList,
                  rcuGasTechList,
                  rcuGasVarConvertitoreList,
                  rcuGasVarPrelAnnuoList,
                  rcuGasVarProfiloList
                )
              )
          }
      }

    measureJoinedWithMassivo
      .mapValues {
        case (
          (flowWithInfoList, externalDailyInfo),
          (
            rcuGasMassivoPList,
            rcuGasTechList,
            rcuGasVarConvertitoreList,
            rcuGasVarPrelAnnuoList,
            rcuGasVarProfiloList
            )
          ) =>
          // shrink rcu size and then associate it to measures
          val newExternalInfo = externalDailyInfo.copy(
            rcuGasMassivoPList = Some(rcuGasMassivoPList),
            rcuGasTechList = Some(rcuGasTechList),
            rcuGasVarConvertitoreList = Some(rcuGasVarConvertitoreList),
            rcuGasVarPrelAnnuoList = Some(rcuGasVarPrelAnnuoList),
            rcuGasVarProfiloList = Some(rcuGasVarProfiloList)
          )

          val newFlowWithInfoList = flowWithInfoList.map { flowWithInfo =>
            flowWithInfo.copy(
              rcuGasTech =
                newExternalInfo.findRcuGasTech(flowWithInfo.flow.date.get),
              rcuGasVarProfilo =
                newExternalInfo.findRcuGasVarProfilo(flowWithInfo.flow.date.get),
              rcuGasVarConvertitore =
                newExternalInfo.findRcuGasVarConvertitore(flowWithInfo.flow.date.get)
            )
          }

          (newFlowWithInfoList, newExternalInfo)
      }

  }

  /** Estrae le info sul distributore, quali il codice remi e l'id regione climatica. */
  def setRcuGasConnessioniDistr2(measures: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))], rcuGasConnessioniDistr2: RDD[RcuGasConnessioniDistr2]): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {
    measures.leftOuterJoin(rcuGasConnessioniDistr2.keyBy(_.tCodicePdr).groupByKey)
      .mapValues({ case ((flowWithInfoList, externalDailyInfo), rcuGasConnessioniDistr2List) =>

        val rcuGasConnessioniDistr2ListFiltered = rcuGasConnessioniDistr2List.map(list => {
          if (flowWithInfoList.nonEmpty) {
            val minDate = flowWithInfoList.head.flow.date.get
            val maxDate = flowWithInfoList.last.flow.date.get
            list.filter(rc =>
              (minDate == rc.dataFineConn || minDate.isBefore(rc.dataFineConn)) &&
                (maxDate == rc.dataInizioConn || maxDate.isAfter(rc.dataInizioConn))
            )
          } else {
            list
          }
        })

        (flowWithInfoList.map(flowWithInfo => {
          val rcuGasConnessioniDistr2 = rcuGasConnessioniDistr2ListFiltered.flatMap(_.find(rc =>
            DateUtility.isBetween(flowWithInfo.flow.date.get, rc.dataInizioConn, rc.dataFineConn)
          ))

          flowWithInfo.copy(idRegioneClimatica = rcuGasConnessioniDistr2.flatMap(_.idRegioneClimatica))
        }), externalDailyInfo.copy(rcuGasConnessioniDistr2List = rcuGasConnessioniDistr2List))
      })
  }

  /** Inserisce nella [[ExternalDailyInfo]] la lista dei PdR sospesi */
  def setSuspendedPdrs(measures: RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))], rcuGasMassivo: RDD[RcuGasMassivoP], suspendedPdrs: RDD[RcuGasSuspendedPdr]): RDD[(String, (List[FlowWithInfo], ExternalDailyInfo))] = {
    val idCodPdrMapping = rcuGasMassivo.map(rcu => (rcu.nIdPdr, rcu.tCodicePdr)).distinct() //get idPdr, codPdr association
    //get codPdr for suspension info
    val codPdrWithSuspension = idCodPdrMapping.join(suspendedPdrs.keyBy(_.nIdPdr).groupByKey()).values //<cod_pdr, suspensionList>
    //return measures with proper suspension info associated
    measures.leftOuterJoin(codPdrWithSuspension)
      .mapValues({ case ((measureList, extInfos), suspendedPdrIter) => (measureList, extInfos.copy(rcuGasSospensioniList = suspendedPdrIter)) })
  }
}
