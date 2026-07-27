package it.eng.au.sgsFlussoStoricoGas.run.perimetro

import it.eng.au.sgsFlussoStoricoGas.controller.perimetro.{PerimetroSwgAController, PerimetroSwgSController, PerimetroUigAController, PerimetroUigSController, PerimetroVtgSController}
import it.eng.au.sgsFlussoStoricoGas.dao.executionTrack.SgsExecutionTrackDao
import it.eng.au.sgsFlussoStoricoGas.dao.perimetro.raw.{SgsPerimetroSwgARawDao, SgsPerimetroSwgSRawDao, SgsPerimetroUigARawDao, SgsPerimetroUigSRawDao, SgsPerimetroVtgSRawDao}
import it.eng.au.sgsFlussoStoricoGas.dao.perimetro.{SgsPerimetroSwgADao, SgsPerimetroSwgSDao, SgsPerimetroUigADao, SgsPerimetroUigSDao, SgsPerimetroVtgSDao}
import it.eng.au.sgsFlussoStoricoGas.dao.rcugas.RcuGasVarTrattamentoPDAO
import it.eng.au.sgsFlussoStoricoGas.model.SgsExecutionTrackModel
import it.eng.au.sgsFlussoStoricoGas.schema.SgsExecutionTrackSchema
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.raw.{SgsPerimetroARawSchema, SgsPerimetroSRawSchema}
import it.eng.au.sgsFlussoStoricoGas.utility.args.FlowArgsFactory
import it.eng.au.sgsFlussoStoricoGas.utility.constants.TipoProcesso
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import it.eng.au.sgsFlussoStoricoGas.utility.log.LogUtility
import it.eng.au.sgsFlussoStoricoGas.utility.log.LogUtility.log
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col

object Driver {
  @transient private lazy val logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {

      val parsedArgs = FlowArgsFactory.parse(args)
      val applicationName = "SGS - Flusso Storico Gas - Perimetro"
      val logName = "SGS LOG:"

      Environment.getOrCreate(applicationName, logName, parsedArgs.pathToProperties)

      LogUtility.printInitialLog()

      runPerimetro()

      LogUtility.printFinalLog()

    } catch {
      case e: Throwable =>
        logger.error(s"Error procedure ${args.mkString(" ")}", e)
        throw e
    }
  }

  private def runPerimetro(): Unit = {

    //Params to run
    val dateToRun = Environment.startDateTime.toString
    val annoEx = Environment.startDateTime.getYear.toString
    val meseEx = Environment.startDateTime.getMonthValue.toString
    val giornoEx = Environment.startDateTime.getDayOfMonth.toString
    val giornoIn = Environment.getProperty("execution.day")
    val activationFlow = if (giornoEx.equals(giornoIn)) true else false
    val boolPerimetroSwgS = Environment.getBoolPerimetroSwgS.toBoolean
    val boolPerimetroSwgA = Environment.getBoolPerimetroSwgA.toBoolean
    val boolPerimetroUigS = Environment.getBoolPerimetroUigS.toBoolean
    val boolPerimetroUigA = Environment.getBoolPerimetroUigA.toBoolean
    val boolPerimetroVtgS = Environment.getBoolPerimetroVtgS.toBoolean
    val pastAnnomese = f"${Environment.startDateTime.minusMonths(1).getYear}${Environment.startDateTime.minusMonths(1).getMonthValue}%02d"

    //Dao
    val rcuGasVarTrattamentoPDAO = new RcuGasVarTrattamentoPDAO
    val perimetroSwgSDao = new SgsPerimetroSwgSDao
    val perimetroSwgADao = new SgsPerimetroSwgADao
    val perimetroUigSDao = new SgsPerimetroUigSDao
    val perimetroUigADao = new SgsPerimetroUigADao
    val perimetroVtgSDao = new SgsPerimetroVtgSDao
    val perimetroSwgSRawDao = new SgsPerimetroSwgSRawDao
    val perimetroSwgARawDao = new SgsPerimetroSwgARawDao
    val perimetroUigSRawDao = new SgsPerimetroUigSRawDao
    val perimetroUigARawDao = new SgsPerimetroUigARawDao
    val perimetroVtgSRawDao = new SgsPerimetroVtgSRawDao
    val executionTrackDao = new SgsExecutionTrackDao

    //Controllers
    val perimetroSwgSController = new PerimetroSwgSController
    val perimetroSwgAController = new PerimetroSwgAController
    val perimetroUigSController = new PerimetroUigSController
    val perimetroUigAController = new PerimetroUigAController
    val perimetroVtgSController = new PerimetroVtgSController

    //Lettura tabelle
    val rcuGasVarTrattamentoDF = rcuGasVarTrattamentoPDAO.get(dateToRun)

    //Data Competenza Giorno Esecuzione Swg e Uig
    val dataCompetenzaSwgUigS = {
      if (activationFlow && boolPerimetroSwgS) {
        perimetroSwgSRawDao.readTable.select(SgsPerimetroSRawSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[", "").replace("]", "")
      }
      else if (activationFlow && boolPerimetroUigS) {
        perimetroUigSRawDao.readTable.select(SgsPerimetroSRawSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[", "").replace("]", "")
      }
      else null
    }

    val dataCompetenzaSwgUigA = {
      if (activationFlow && boolPerimetroSwgA) {
        perimetroSwgARawDao.readTable.select(SgsPerimetroARawSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[", "").replace("]", "")
      }
      else if (activationFlow && boolPerimetroUigA) {
        perimetroUigARawDao.readTable.select(SgsPerimetroARawSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[", "").replace("]", "")
      }
      else null
    }

    //Data competenza Giorno Esecuzione Vtg
    val dataCompetenzaVtg = {
      if (boolPerimetroVtgS) {
        perimetroVtgSRawDao.readTable.select(SgsPerimetroSRawSchema.d_data_decorrenza.toString).distinct.rdd.collect.mkString.replace("[", "").replace("]", "")
      }
      else null
    }

    //Calcolo perimetro
    //perimetro Swg S
    if (activationFlow && boolPerimetroSwgS) {
      val perimetroSwgSRawDF = perimetroSwgSRawDao.readTable
      val perimetroSwgSDF = perimetroSwgSController.getPerimetroS(perimetroSwgSRawDF, rcuGasVarTrattamentoDF, annoEx, meseEx, giornoEx)

      perimetroSwgSDao.writeParquet(perimetroSwgSDF)

      perimetroSwgSRawDF.unpersist(blocking = true)
      perimetroSwgSDF.unpersist(blocking = true)
    }

    //perimetro Swg A
    if (activationFlow && boolPerimetroSwgA) {
      val perimetroSwgARawDF = perimetroSwgARawDao.readTable
      val perimetroSwgSLastVersionDF = perimetroSwgSDao.readLastExecutionIdLastAnnomeseDF(pastAnnomese)
      val perimetroSwgADF = perimetroSwgAController.getPerimetroA(perimetroSwgARawDF, rcuGasVarTrattamentoDF, perimetroSwgSLastVersionDF, annoEx, meseEx, giornoEx)

      perimetroSwgADao.writeParquet(perimetroSwgADF)

      perimetroSwgARawDF.unpersist(blocking = true)
      perimetroSwgSLastVersionDF.unpersist(blocking = true)
      perimetroSwgADF.unpersist(blocking = true)
    }

    //perimetro Uig S
    if (activationFlow && boolPerimetroUigS) {
      val perimetroUigSRawDF = perimetroUigSRawDao.readTable
      val perimetroUigSDF = perimetroUigSController.getPerimetroS(perimetroUigSRawDF, rcuGasVarTrattamentoDF, annoEx, meseEx, giornoEx)

      perimetroUigSDao.writeParquet(perimetroUigSDF)

      perimetroUigSRawDF.unpersist(blocking = true)
      perimetroUigSDF.unpersist(blocking = true)
    }

    //perimetro Uig A
    if (activationFlow && boolPerimetroUigA) {
      val perimetroUigARawDF = perimetroUigARawDao.readTable
      val perimetroUigSLastVersionDF = perimetroUigSDao.readLastExecutionIdLastAnnomeseDF(pastAnnomese)
      val perimetroUigADF = perimetroUigAController.getPerimetroA(perimetroUigARawDF, rcuGasVarTrattamentoDF, perimetroUigSLastVersionDF, annoEx, meseEx, giornoEx)

      perimetroUigADao.writeParquet(perimetroUigADF)

      perimetroUigARawDF.unpersist(blocking = true)
      perimetroUigSLastVersionDF.unpersist(blocking = true)
      perimetroUigADF.unpersist(blocking = true)
    }

    //perimetro Vtg S
    if (boolPerimetroVtgS) {
      val perimetroVtgSRawDF = perimetroVtgSRawDao.readTable
      val perimetroVtgSDF = perimetroVtgSController.getPerimetroS(perimetroVtgSRawDF, rcuGasVarTrattamentoDF, annoEx, meseEx, giornoEx)

      perimetroVtgSDao.writeParquet(perimetroVtgSDF)

      perimetroVtgSRawDF.unpersist(blocking = true)
      perimetroVtgSDF.unpersist(blocking = true)
    }

    //Registro operazione nella execution track
    val sgsExecutionTrackDF = Environment.getSpark.sqlContext.createDataFrame(
      Seq(
        SgsExecutionTrackModel(
          TipoProcesso.P.toString, dataCompetenzaSwgUigS, Environment.executionId, Environment.startDateTime.toString
        ),
        SgsExecutionTrackModel(
          TipoProcesso.P.toString, dataCompetenzaSwgUigA, Environment.executionId, Environment.startDateTime.toString
        ),
        SgsExecutionTrackModel(
          TipoProcesso.P.toString, dataCompetenzaVtg, Environment.executionId, Environment.startDateTime.toString
        )
      )
    ).filter(col(SgsExecutionTrackSchema.d_data_competenza).isNotNull).distinct

    executionTrackDao.writeParquet(sgsExecutionTrackDF)
  }

}

