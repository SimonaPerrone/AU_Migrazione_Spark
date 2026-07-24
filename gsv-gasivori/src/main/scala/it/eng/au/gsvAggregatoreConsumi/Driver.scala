package it.eng.au.gsvAggregatoreConsumi

import it.eng.au.gsvAggregatoreConsumi.controller.{AggregatoreController, JoinRichiesteFornitureController, SplitRichiesteController, SterilizationController}
import it.eng.au.gsvAggregatoreConsumi.dao.agg.{DailyConsumptionDAO, DailyConsumptionEsclusiDAO, DailyConsumptionIncoerentiDAO}
import it.eng.au.gsvAggregatoreConsumi.dao.gsv.{GsvConsAggrDAO, GsvConsFornitureDAO, GsvConsRichiestaDAO}
import it.eng.au.gsvAggregatoreConsumi.utility.environment.Environment
import it.eng.au.gsvAggregatoreConsumi.utility.log.LogUtility
import it.eng.au.gsvAggregatoreConsumi.args.ArgsFactory
import org.apache.log4j.Logger

object Driver {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)

  def main(args:Array[String]):Unit={
    try{
      val parsedArgs=ArgsFactory.parse(args)

      val applicationName="[GSV] Gasivori - Aggregatore"
      val logName="GSV LOG:"

      Environment.getOrCreate(applicationName, logName, parsedArgs.propertiesPath)

      LogUtility.printInitialLog()

      run()

      LogUtility.printFinalLog()
    }
    catch {
      case e:Throwable=>
        logger.error(s"An error occurred in the procedure.")
        throw e
    }


  }

  def run():Unit={

    //Inizializzo i DAO
    val gsvFornitureDao = new GsvConsFornitureDAO
    val gsvRichiestaDao = new GsvConsRichiestaDAO
    val gsvAggrDao = new GsvConsAggrDAO

    val dailyConsumptionDAO = new DailyConsumptionDAO
    val dailyConsumptionEsclusiDAO = new DailyConsumptionEsclusiDAO
    val dailyConsumptionIncoerentiDAO = new DailyConsumptionIncoerentiDAO

    //Inizializzo i controller
    val joinRichiesteFornitureController = new JoinRichiesteFornitureController
    val sterilizationController = new SterilizationController
    val splitRichiesteController = new SplitRichiesteController
    val aggregatoreController = new AggregatoreController

    //Lettura tabelle
    val gsvForniture = gsvFornitureDao.get()
    val gsvRichiesta = gsvRichiestaDao.get()

    val dailyConsumption = dailyConsumptionDAO.readParquet()
    val dailyConsumptionEsclusi = dailyConsumptionEsclusiDAO.readParquet()
    val dailyConsumptionIncoerenti = dailyConsumptionIncoerentiDAO.readParquet()

    //Genero Daily Consumption in input ad aggregazione
    val consumptionDF = sterilizationController.sterilizeConsumptions(dailyConsumption, dailyConsumptionIncoerenti, dailyConsumptionEsclusi)

    //Associo la richiesta alle forniture
    val fornitureFilteredDF = joinRichiesteFornitureController.JoinRichiesteForniture(gsvRichiesta, gsvForniture)

    //Creo 3 dataset differenti per gestire le 3 differenti tipologie di richiesta
    val fornitureROrdi = splitRichiesteController.getOrdinarie(fornitureFilteredDF)
    val fornitureRSuppl = splitRichiesteController.getSuppletive(fornitureFilteredDF)
    val fornitureRCons = splitRichiesteController.getConsuntive(fornitureFilteredDF)

    //Aggregazione dei consumi
    val aggOrdinarieDF = aggregatoreController.aggregationFunction(consumptionDF, fornitureROrdi)
    val aggSuppletiveDF = aggregatoreController.aggregationFunction(consumptionDF, fornitureRSuppl)
    val aggConsuntiveDF = aggregatoreController.aggregationFunction(consumptionDF, fornitureRCons)

    //Union dei risultati
    val aggUnitedDF = aggOrdinarieDF
      .unionByName(aggSuppletiveDF)
      .unionByName(aggConsuntiveDF)
      .toDF()

    //Scrittura su hive
    gsvAggrDao.writeOnHive(aggUnitedDF)

  }
}
