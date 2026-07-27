package it.eng.au.sgsFlussoStoricoGas.run.aggregazione

import it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udb.{UdbSwgAAggregationController, UdbSwgSAggregationController, UdbUigAAggregationController, UdbUigSAggregationController, UdbVtgAAggregationController, UdbVtgSAggregationController}
import it.eng.au.sgsFlussoStoricoGas.controller.aggregazioni.udd.{UddSwgAAggregationController, UddSwgSAggregationController, UddUigAAggregationController, UddUigSAggregationController, UddVtgAAggregationController, UddVtgSAggregationController}
import it.eng.au.sgsFlussoStoricoGas.controller.{PrevalidationController, SterilizationController}
import it.eng.au.sgsFlussoStoricoGas.dao.aggregazione.AggregatoreInfoDettDao
import it.eng.au.sgsFlussoStoricoGas.dao.dailyConsumption.{DailyConsumptionAggDao, DailyConsumptionAggEsclusiDao, DailyConsumptionAggIncoerentiGdMDao}
import it.eng.au.sgsFlussoStoricoGas.dao.executionTrack.SgsExecutionTrackDao
import it.eng.au.sgsFlussoStoricoGas.dao.perimetro.{SgsPerimetroSwgADao, SgsPerimetroSwgSDao, SgsPerimetroUigADao, SgsPerimetroUigSDao, SgsPerimetroVtgSDao}
import it.eng.au.sgsFlussoStoricoGas.dao.rcugas.{RcuGasConnessioniDistr2PDao, RcuGasVarConvertitorePDao, RcuGasVarMisuratorePDao}
import it.eng.au.sgsFlussoStoricoGas.model.SgsExecutionTrackModel
import it.eng.au.sgsFlussoStoricoGas.schema.SgsExecutionTrackSchema
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.utility.args.FlowArgsFactory
import it.eng.au.sgsFlussoStoricoGas.utility.constants.TipoProcesso
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import it.eng.au.sgsFlussoStoricoGas.utility.file.FileUtility.safeReadParquet
import it.eng.au.sgsFlussoStoricoGas.utility.log.LogUtility
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {

      val parsedArgs = FlowArgsFactory.parse(args)
      val applicationName = "SGS - Flusso Storico Gas - Aggregazione"
      val logName = "SGS LOG:"

      Environment.getOrCreate(applicationName, logName, parsedArgs.pathToProperties)

      LogUtility.printInitialLog()

      runAggregazione()

      LogUtility.printFinalLog()

    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }

  private def runAggregazione(): Unit = {

    //Params to run
    val dateToRun = Environment.startDateTime.toString
    val giornoEx = Environment.startDateTime.getDayOfMonth.toString
    val giornoIn = Environment.getProperty("execution.day")
    val giornoInVtgA = Environment.getProperty("execution.day.vtga")
    val activationFlow = if (giornoEx.equals(giornoIn)) true else false
    val activationFlowVtgA = if(giornoEx.equals(giornoInVtgA)) true else false
    val boolUdbAggregazioneSwgS = Environment.getBoolAggregazioneUdbSwgS.toBoolean
    val boolUdbAggregazioneSwgA = Environment.getBoolAggregazioneUdbSwgA.toBoolean
    val boolUdbAggregazioneUigS = Environment.getBoolAggregazioneUdbUigS.toBoolean
    val boolUdbAggregazioneUigA = Environment.getBoolAggregazioneUdbUigA.toBoolean
    val boolUdbAggregazioneVtgS = Environment.getBoolAggregazioneUdbVtgS.toBoolean
    val boolUdbAggregazioneVtgA = Environment.getBoolAggregazioneUdbVtgA.toBoolean
    val boolUddAggregazioneSwgS = Environment.getBoolAggregazioneUddSwgS.toBoolean
    val boolUddAggregazioneSwgA = Environment.getBoolAggregazioneUddSwgA.toBoolean
    val boolUddAggregazioneUigS = Environment.getBoolAggregazioneUddUigS.toBoolean
    val boolUddAggregazioneUigA = Environment.getBoolAggregazioneUddUigA.toBoolean
    val boolUddAggregazioneVtgS = Environment.getBoolAggregazioneUddVtgS.toBoolean
    val boolUddAggregazioneVtgA = Environment.getBoolAggregazioneUddVtgA.toBoolean

    //Dao
    //Consumptions
    val dailyConsumptionAggDao = new DailyConsumptionAggDao
    val dailyConsumptionAggIncoerentiGdMDao = new DailyConsumptionAggIncoerentiGdMDao
    val dailyConsumptionAggEsclusiDao = new DailyConsumptionAggEsclusiDao

    //Rcugas
    val rcuGasVarMisuratorePDao = new RcuGasVarMisuratorePDao
    val rcuGasVarConvertitorePDao = new RcuGasVarConvertitorePDao
    val rcuGasConnessioniDistr2PDao = new RcuGasConnessioniDistr2PDao

    //Perimetro
    val perimetroSwgSDao = new SgsPerimetroSwgSDao
    val perimetroSwgADao = new SgsPerimetroSwgADao
    val perimetroUigSDao = new SgsPerimetroUigSDao
    val perimetroUigADao = new SgsPerimetroUigADao
    val perimetroVtgSDao = new SgsPerimetroVtgSDao

    //Exec track
    val executionTrackDao = new SgsExecutionTrackDao

    //Aggregatori
    val aggregatoreInfoDettDao = new AggregatoreInfoDettDao

    //Controller
    val sterilizationController = new SterilizationController
    val prevalidationController = new PrevalidationController
    //UDB
    val udbSwgSAggregationController = new UdbSwgSAggregationController
    val udbSwgAAggregationController = new UdbSwgAAggregationController
    val udbUigSAggregationController = new UdbUigSAggregationController
    val udbUigAAggregationController = new UdbUigAAggregationController
    val udbVtgSAggregationController = new UdbVtgSAggregationController
    val udbVtgAAggregationController = new UdbVtgAAggregationController
    //UDD
    val uddSwgSAggregationController = new UddSwgSAggregationController
    val uddSwgAAggregationController = new UddSwgAAggregationController
    val uddUigSAggregationController = new UddUigSAggregationController
    val uddUigAAggregationController = new UddUigAAggregationController
    val uddVtgSAggregationController = new UddVtgSAggregationController
    val uddVtgAAggregationController = new UddVtgAAggregationController

    //Get Rcugas
    val rcuGasVarMisuratoreDF = rcuGasVarMisuratorePDao.get(dateToRun)
    val rcuGasVarConvertitoreDF = rcuGasVarConvertitorePDao.get(dateToRun)
    val rcuGasConnessioniDF = rcuGasConnessioniDistr2PDao.get(dateToRun)

    val perimetroSchema = StructType(SgsPerimetroSchema.getValues.map(fieldName => StructField(fieldName, StringType, nullable = true)))
    val perimtroEmptyDF: DataFrame = Environment.getSpark.createDataFrame(Environment.getSpark.sparkContext.emptyRDD[org.apache.spark.sql.Row], perimetroSchema)

    //Get Consumptions
    val dailyConsumption = dailyConsumptionAggDao.readTable
    val dailyIncoerenti = dailyConsumptionAggIncoerentiGdMDao.readTable
    val dailyEsclusi = dailyConsumptionAggEsclusiDao.readTable
    val consumptionsDF = sterilizationController.sterilizeConsumptions(dailyConsumption, dailyIncoerenti, dailyEsclusi)
    val dataframesPerimetro = Seq(
      if (activationFlow && (boolUdbAggregazioneSwgS || boolUddAggregazioneSwgS) && safeReadParquet(Environment.getPerimetroSwgSPath)) Some(perimetroSwgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)) else None,
      if (activationFlow && (boolUdbAggregazioneSwgA || boolUddAggregazioneSwgA) && safeReadParquet(Environment.getPerimetroSwgAPath)) Some(perimetroSwgADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)) else None,
      if (activationFlow && (boolUdbAggregazioneUigS || boolUddAggregazioneUigS) && safeReadParquet(Environment.getPerimetroUigSPath)) Some(perimetroUigSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)) else None,
      if (activationFlow && (boolUdbAggregazioneUigA || boolUddAggregazioneUigA) && safeReadParquet(Environment.getPerimetroUigAPath)) Some(perimetroUigADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)) else None,
      if ((boolUdbAggregazioneVtgS || boolUddAggregazioneVtgS) && safeReadParquet(Environment.getPerimetroVtgSPath)) Some(perimetroVtgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)) else None,
      if (activationFlowVtgA && (boolUdbAggregazioneVtgA || boolUddAggregazioneVtgA) && safeReadParquet(Environment.getPerimetroVtgSPath)) Some(perimetroVtgSDao.readLastForVtgAAggr(executionTrackDao.getExecIdForVtgAAggregation)) else None
    ).flatten

    val perimetroFull = dataframesPerimetro match {
      case Nil => perimtroEmptyDF  // Se nessuna condizione è vera, restituisce un DataFrame vuoto
      case head :: tail => tail.foldLeft(head)(_.unionByName(_))
    }

    val cachedConsumptionsDF = udbSwgSAggregationController.getReducedConsumptions(perimetroFull, consumptionsDF)

    cachedConsumptionsDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
    consumptionsDF.unpersist(blocking = true)
    dailyConsumption.unpersist(blocking = true)
    dailyIncoerenti.unpersist(blocking = true)
    dailyEsclusi.unpersist(blocking = true)

    //Data Competenza Giorno Esecuzione Swg e Uig
    val dataCompetenzaSwgUigS = {
      if (activationFlow && (boolUdbAggregazioneSwgS || boolUddAggregazioneSwgS)) {
        perimetroSwgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation).select(SgsPerimetroSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[","").replace("]", "")
      }
      else if (activationFlow && (boolUdbAggregazioneUigS || boolUddAggregazioneUigS)) {
        perimetroUigSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation).select(SgsPerimetroSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[","").replace("]", "")
      }
      else null
    }

    val dataCompetenzaSwgUigA = {
      if (activationFlow && (boolUdbAggregazioneSwgA || boolUddAggregazioneSwgA)) {
        perimetroSwgADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation).select(SgsPerimetroSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[","").replace("]", "")
      }
      else if (activationFlow && (boolUdbAggregazioneUigA || boolUddAggregazioneUigA)) {
        perimetroUigADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation).select(SgsPerimetroSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[","").replace("]", "")
      }
      else null
    }

    val dataCompetenzaVtgS = {
      if ((boolUdbAggregazioneVtgS || boolUddAggregazioneVtgS) && safeReadParquet(Environment.getPerimetroVtgSPath)) {
        perimetroVtgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation).select(SgsPerimetroSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[","").replace("]", "")
      }
      else null
    }

    val dataCompetenzaVtgA = {
      if (activationFlowVtgA && (boolUdbAggregazioneVtgA || boolUddAggregazioneVtgA)) {
        perimetroVtgSDao.readLastForVtgAAggr(executionTrackDao.getExecIdForVtgAAggregation).select(SgsPerimetroSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[","").replace("]", "")
      }
      else null
    }

    //Get aggregations
    //UDB
    if (boolUdbAggregazioneSwgS && activationFlow) {
      val perimetroSwgS = perimetroSwgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = udbSwgSAggregationController.getAggregatoreInfoDett(perimetroSwgS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroSwgS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUdbAggregazioneSwgA && activationFlow) {
      val perimetroSwgA = perimetroSwgADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = udbSwgAAggregationController.getAggregatoreInfoDett(perimetroSwgA, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroSwgA.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUdbAggregazioneUigS && activationFlow) {
      val perimetroUigS = perimetroUigSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = udbUigSAggregationController.getAggregatoreInfoDett(perimetroUigS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroUigS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUdbAggregazioneUigA && activationFlow) {
      val perimetroUigA = perimetroUigADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = udbUigAAggregationController.getAggregatoreInfoDett(perimetroUigA, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroUigA.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUdbAggregazioneVtgS && safeReadParquet(Environment.getPerimetroVtgSPath)) {
      val perimetroVtgS = perimetroVtgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = udbVtgSAggregationController.getAggregatoreInfoDett(perimetroVtgS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroVtgS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUdbAggregazioneVtgA && activationFlowVtgA) {
      val executionIdVtgA = executionTrackDao.getExecIdForVtgAAggregation
      val perimetroVtgS = perimetroVtgSDao.readLastForVtgAAggr(executionIdVtgA)
      val aggregatoreInfoDett = udbVtgAAggregationController.getAggregatoreInfoDett(perimetroVtgS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroVtgS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    //UDD
    if (boolUddAggregazioneSwgS && activationFlow) {
      val perimetroSwgS = perimetroSwgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = uddSwgSAggregationController.getAggregatoreInfoDett(perimetroSwgS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroSwgS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUddAggregazioneSwgA && activationFlow) {
      val perimetroSwgA = perimetroSwgADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = uddSwgAAggregationController.getAggregatoreInfoDett(perimetroSwgA, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroSwgA.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUddAggregazioneUigS && activationFlow) {
      val perimetroUigS = perimetroUigSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = uddUigSAggregationController.getAggregatoreInfoDett(perimetroUigS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroUigS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUddAggregazioneUigA && activationFlow) {
      val perimetroUigA = perimetroUigADao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = uddUigAAggregationController.getAggregatoreInfoDett(perimetroUigA, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroUigA.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUddAggregazioneVtgS && safeReadParquet(Environment.getPerimetroVtgSPath)) {
      val perimetroVtgS = perimetroVtgSDao.readLastVersionDF(executionTrackDao.getExecIdForAggregation)
      val aggregatoreInfoDett = uddVtgSAggregationController.getAggregatoreInfoDett(perimetroVtgS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroVtgS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    if (boolUddAggregazioneVtgA && activationFlowVtgA) {
      val executionIdVtgA = executionTrackDao.getExecIdForVtgAAggregation
      val perimetroVtgS = perimetroVtgSDao.readLastForVtgAAggr(executionIdVtgA)
      val aggregatoreInfoDett = uddVtgAAggregationController.getAggregatoreInfoDett(perimetroVtgS, cachedConsumptionsDF, rcuGasVarMisuratoreDF, rcuGasVarConvertitoreDF, rcuGasConnessioniDF)
      val aggregatorePreval = prevalidationController.executePrevalidation(aggregatoreInfoDett)
      aggregatoreInfoDettDao.write(aggregatorePreval)
      perimetroVtgS.unpersist(blocking = true)
      aggregatoreInfoDett.unpersist(blocking = true)
      aggregatorePreval.unpersist(blocking = true)
    }

    val sgsExecutionTrackDF = Environment.getSpark.sqlContext.createDataFrame(
      Seq(
        SgsExecutionTrackModel(
          TipoProcesso.A.toString, dataCompetenzaSwgUigS, Environment.executionId, Environment.startDateTime.toString
        ),
        SgsExecutionTrackModel(
          TipoProcesso.A.toString, dataCompetenzaSwgUigA, Environment.executionId, Environment.startDateTime.toString
        ),
        SgsExecutionTrackModel(
          TipoProcesso.A.toString, dataCompetenzaVtgS, Environment.executionId, Environment.startDateTime.toString
        ),
        SgsExecutionTrackModel(
          TipoProcesso.A.toString, dataCompetenzaVtgA, Environment.executionId, Environment.startDateTime.toString
        )
      )
    ).filter(col(SgsExecutionTrackSchema.d_data_competenza).isNotNull).distinct

    executionTrackDao.writeParquet(sgsExecutionTrackDF)
  }
}
