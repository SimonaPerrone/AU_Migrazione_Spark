package it.eng.au.aggiustamentoGas.controller

import it.eng.au.aggiustamentoGas.dao.rcu.RcuAziendaPDAO
import it.eng.au.aggiustamentoGas.dao.rcugas._
import it.eng.au.aggiustamentoGas.model.agg._
import it.eng.au.aggiustamentoGas.schema.rcu.RcuAziendaPSchema
import it.eng.au.aggiustamentoGas.schema.rcugas._
import it.eng.au.aggiustamentoGas.utility.environment.Environment
import it.eng.au.aggiustamentoGas.utility.parsedate.DateUtility
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.joda.time.format.DateTimeFormat

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

class JoinAnagraficaConsumptionController {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def readRcuGasBilanciamentoP: DataFrame = new RcuGasBilanciamentoPDAO().readParquet

  def readRcuGasGestTrasportoP: DataFrame = new RcuGasGestTrasportoPDAO().readParquet

  def readRcuGasItP: DataFrame = new RcuGasItPDAO().readParquet

  def readRcuGasRemiAggregazioneP: DataFrame = new RcuGasRemiAggregazionePDAO().readParquet

  def readRcuGasRemiAnagraficaP: DataFrame = new RcuGasRemiAnagraficaPDAO().readParquet

  def readRcuGasUdbP: DataFrame = new RcuGasUdbPDAO().readParquet

  def readMassivo: DataFrame = new RcuGasMassivoPDAO().readParquet

  def readDistributoreP: DataFrame = new RcuGasDistributorePDAO().readParquet

  def readRcuAziendaP: DataFrame = new RcuAziendaPDAO().readParquet

  def getJoinedAnagrafica(consumptions: RDD[(String, (List[Consumption], ExternalDailyInfo))], startDate: String, endDate: String, session: String, monthTreatment: RDD[MonthTreatment], sqoopDate: String): RDD[(DailyConsumption, ExternalDailyInfo)] = {
    val distr = readDistributoreP.select(
      RcuGasDistributorePSchema.n_id_distributore,
      RcuGasDistributorePSchema.t_piva
    ).distinct()

    val distrBroad = Environment.getSpark.sparkContext.broadcast(
      distr.rdd.map(r => (r.getAs[String](RcuGasDistributorePSchema.n_id_distributore), r.getAs[String](RcuGasDistributorePSchema.t_piva))).collectAsMap()
    )

    val conn2 = Environment.getSpark.sparkContext.broadcast(
      getConn2(startDate, endDate).keyBy(_.tRemi).groupByKey().collectAsMap()
    )

    val bill2 = getBill2(startDate, endDate).keyBy(_.tCodicePdr).groupByKey()

    consumptions.leftOuterJoin(bill2)
      .leftOuterJoin(monthTreatment.keyBy(_.pdr).groupByKey)
      //.repartition(Environment.getNumberPartition.toInt * 6)
      .flatMap({ case (pdr, (((consumptions, externalDailyInfo), bill2List), monthTreatmentList)) =>
        consumptions.map(c => {
          val sqoopDt = DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(sqoopDate).withTimeAtStartOfDay()
          //          val massivoAtSqoopDate = externalDailyInfo.findRcuGasMassivoP(sqoopDt)
          val massivo = externalDailyInfo.findRcuGasMassivoP(c.date)
          val connessioniDistr2 = externalDailyInfo.findRcuGasConnessioniDistr2(c.date)
          val rcuGasTech = externalDailyInfo.findRcuGasTech(c.date)
          val bill2Current = bill2List.getOrElse(Iterable()).find(b =>
            DateUtility.isBetween(c.date, b.dDataInizioBil, b.dDataFineBil)
          )
          val conn2List = connessioniDistr2.flatMap(some => conn2.value.get(some.tRemi))
          val conn2Current = conn2List.getOrElse(Iterable()).find(b =>
            DateUtility.isBetween(c.date, b.dDataInizioAgg, b.dDataFineAgg)
          )
          val mTreatment = monthTreatmentList.flatMap(_.find(monthTreatment => monthTreatment.month.equals(c.date.toString("yyyyMM"))))

          val pivaDistr = connessioniDistr2.flatMap(some => distrBroad.value.get(some.nIdDistr))

          val dailyC = new DailyConsumption(
            pdr = pdr,
            date = new Timestamp(c.date.getMillis),
            value = c.value,
            pprof = c.pprof,
            coefficient = c.coefficient,
            ca = c.ca,
            idRegClim = c.idRegClim,
            codProfStd = c.codProfStd,
            segnante = c.segnante,
            idFormula = c.idFormula,
            errorCode = if (mTreatment.map(_.treatment).isDefined) ErrorEnum.getMaxPriorityError(c.errorCode.filter(c => c != ErrorEnum.TREATMENT_IS_NULL_ERROR_CODE)).id else ErrorEnum.getMaxPriorityError(c.errorCode).id,
            //errorCode = if (mTreatment.map(_.treatment).getOrElse("").equals("N")) ErrorEnum.getMaxPriorityError(c.errorCode ++ Array(ErrorEnum.TREATMENT_IS_N_ERROR_CODE)).id else ErrorEnum.getMaxPriorityError(c.errorCode).id,
            pivaDistr = pivaDistr,
            pivaUdd = massivo.flatMap(_.pivaUdd),
            pivaUdb = bill2Current.map(_.tPivaUdb),
            pivaIt = conn2Current.map(_.tPivaIt),
            pivaRdb = Some("10238291008"),
            dtg = bill2Current.map(_.tPivaUdb match {
              case "10238291008" => "Y"
              case _ => "N"
            }),
            codRemi = conn2Current.map(_.tRemi),
            tipoCliente = massivo.map(some =>
              if (some.pivaUdd.isDefined && pivaDistr.isDefined && some.pivaUdd.get == pivaDistr.get) "A"
              else if (Set("02", "03", "M2", "M3").contains(some.tTipoFornitura.getOrElse(""))) "D"
              else "U"),
            unitMisPrel = Some("SM3"),
            annoMese = Option(mTreatment.map(_.month).getOrElse(c.date.toString("yyyyMM"))),
            session = session,
            treatment = mTreatment.map(_.treatment),
            causale = if (c.errorCode.toSet.contains(ErrorEnum.PDR_WITHOUT_SEGMENTS_ERROR_CODE) || c.errorCode.toSet.contains(ErrorEnum.TREATMENT_IS_N_ERROR_CODE) || mTreatment.map(_.treatment).getOrElse("").equals("N")) Some("M")
            else if (ErrorEnum.getMaxPriorityError(c.errorCode) != ErrorEnum.NO_ERROR_CODE) Some("T")
            else None,
            isValid = !c.isPdrSuspended,
            leftMeasureLocalFile = Option(c.startMeasure).flatMap(_.flow.localFile),
            rightMeasureLocalFile = Option(c.endMeasure).flatMap(_.flow.localFile),
            tCodIstat = externalDailyInfo.getRcuGasMassivoPWithMaxDataFineForn.flatMap(r => if (r.tComuneIstatPdr.isEmpty) r.tComuneIstatForn else r.tComuneIstatPdr),
            classeMisuratore = rcuGasTech.flatMap(_.classeMisuratore),
            valueNotSterilized = c.valueNotSterilized,
            valueF3 = c.valueF3,
            startDateF2 = c.dateStartF2,
            endDateF2 = c.dateEndF2
          )
          (dailyC, externalDailyInfo)
        })
      })
  }

  def filterPeriodCol(startcolName: String, endColName: String, startDate: String, endDate: String): Column =
    date_format(coalesce(col(startcolName), lit("1492-12-31 00:00:00.0")), "yyyy-MM-dd") <=
      date_add(add_months(from_unixtime(unix_timestamp(lit(endDate), "yyyyMM"), "yyyy-MM-dd"), 1), -1) &&
      date_format(coalesce(col(endColName), lit("2999-12-31 00:00:00.0")), "yyyy-MM-dd") >=
        from_unixtime(unix_timestamp(lit(startDate), "yyyyMM"), "yyyy-MM-dd")

  def getBill2(startDate: String, endDate: String): RDD[Bill2] = {
    val bilanciamento = readRcuGasBilanciamentoP.where(filterPeriodCol(RcuGasBilanciamentoPSchema.d_data_inizio, RcuGasBilanciamentoPSchema.d_data_fine, startDate, endDate))
    val udb = readRcuGasUdbP
    val massivo = readMassivo
    val azienda = readRcuAziendaP

    bilanciamento
      .join(massivo.select(RcuGasMassivoPSchema.n_id_pdr, RcuGasMassivoPSchema.t_codice_pdr).distinct, bilanciamento(RcuGasBilanciamentoPSchema.n_id_pdr) === massivo(RcuGasMassivoPSchema.n_id_pdr))
      .join(broadcast(udb), bilanciamento(RcuGasBilanciamentoPSchema.n_id_udb) === udb(RcuGasUdbPSchema.n_id_udb))
      .join(broadcast(azienda), udb(RcuGasUdbPSchema.n_id_azienda) === azienda(RcuAziendaPSchema.n_id_azienda))
      .select(
        massivo(RcuGasMassivoPSchema.t_codice_pdr),
        bilanciamento(RcuGasBilanciamentoPSchema.n_id_pdr),
        bilanciamento(RcuGasBilanciamentoPSchema.n_id_udb),
        udb(RcuGasUdbPSchema.n_id_azienda),
        azienda(RcuAziendaPSchema.t_piva),
        bilanciamento(RcuGasBilanciamentoPSchema.d_data_inizio),
        bilanciamento(RcuGasBilanciamentoPSchema.d_data_fine)
      ).distinct
      .rdd
      .map(r => {
        val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")
        Bill2(
          tCodicePdr = r.getAs[String](RcuGasMassivoPSchema.t_codice_pdr),
          nIdPdr = r.getAs[String](RcuGasBilanciamentoPSchema.n_id_pdr),
          nIdUdb = r.getAs[String](RcuGasBilanciamentoPSchema.n_id_udb),
          nIdAziendaUdb = r.getAs[String](RcuGasUdbPSchema.n_id_azienda),
          tPivaUdb = r.getAs[String](RcuAziendaPSchema.t_piva),
          dDataInizioBil = Try(formatter.parseLocalDate(r.getAs[String](RcuGasBilanciamentoPSchema.d_data_inizio)).toDateTimeAtStartOfDay) match {
            case Success(value) => value
            case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01 00:00:00.0").toDateTimeAtStartOfDay
          },
          dDataFineBil = Try(formatter.parseLocalDate(r.getAs[String](RcuGasBilanciamentoPSchema.d_data_fine)).toDateTimeAtStartOfDay) match {
            case Success(value) => value
            case Failure(_: NullPointerException) => formatter.parseLocalDate("2999-12-31 23:59:59.0").toDateTimeAtStartOfDay
          }
        )
      })
  }

  def getConn2(startDate: String, endDate: String): RDD[Conn2] = {
    val remiAngrafica = broadcast(readRcuGasRemiAnagraficaP)
    val remiAggregazione = readRcuGasRemiAggregazioneP.where(filterPeriodCol(RcuGasRemiAggregazionePSchema.d_data_inizio, RcuGasRemiAggregazionePSchema.d_data_fine, startDate, endDate))
    val gestTrasporto = readRcuGasGestTrasportoP
    val it = readRcuGasItP
    val azienda = readRcuAziendaP

    remiAggregazione
      .join(remiAngrafica.as("fisico"), remiAggregazione(RcuGasRemiAggregazionePSchema.n_id_remi_anagrafica_fisico) === remiAngrafica(RcuGasRemiAnagraficaPSchema.n_id_remi_anagrafica))
      .join(remiAngrafica.as("pool"), remiAggregazione(RcuGasRemiAggregazionePSchema.n_id_remi_anagrafica_pool) === remiAngrafica(RcuGasRemiAnagraficaPSchema.n_id_remi_anagrafica))
      .join(broadcast(gestTrasporto), col("pool." + RcuGasRemiAnagraficaPSchema.n_id_remi_anagrafica) === gestTrasporto(RcuGasGestTrasportoPSchema.n_id_remi_anagrafica))
      .join(broadcast(it), it(RcuGasItPSchema.n_id_it) === gestTrasporto(RcuGasGestTrasportoPSchema.n_id_it))
      .join(broadcast(azienda), it(RcuGasItPSchema.n_id_azienda) === azienda(RcuAziendaPSchema.n_id_azienda))
      .select(
        col("pool." + RcuGasRemiAnagraficaPSchema.t_remi).as(RcuGasRemiAnagraficaPSchema.t_remi),
        it(RcuGasItPSchema.n_id_azienda),
        azienda(RcuAziendaPSchema.t_piva),
        remiAggregazione(RcuGasRemiAggregazionePSchema.d_data_inizio),
        remiAggregazione(RcuGasRemiAggregazionePSchema.d_data_fine)
      ).rdd.distinct.map(r => {
      val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")
      Conn2(
        tRemi = r.getAs[String](RcuGasRemiAnagraficaPSchema.t_remi),
        nIdAziendaIt = r.getAs[String](RcuGasItPSchema.n_id_azienda),
        tPivaIt = r.getAs[String](RcuAziendaPSchema.t_piva),
        dDataInizioAgg = Try(formatter.parseLocalDate(r.getAs[String](RcuGasRemiAggregazionePSchema.d_data_inizio)).toDateTimeAtStartOfDay) match {
          case Success(value) => value
          case Failure(_: NullPointerException) => formatter.parseLocalDate("1970-01-01 00:00:00.0").toDateTimeAtStartOfDay
        },
        dDataFineAgg = Try(formatter.parseLocalDate(r.getAs[String](RcuGasRemiAggregazionePSchema.d_data_fine)).toDateTimeAtStartOfDay) match {
          case Success(value) => value
          case Failure(_: NullPointerException) => formatter.parseLocalDate("2999-12-31 23:59:59.0").toDateTimeAtStartOfDay
        }
      )
    })
  }
}
