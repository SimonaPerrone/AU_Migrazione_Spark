package it.eng.au.ERP

import it.eng.au.ERP.flow.DIST.CalcoloPrelevatoPuntiPrelievoOrariDISTFlow
import it.eng.au.ERP.flow.IP.CalcoloPrelevatoPuntiPrelievoOrariIPFlow
import it.eng.au.ERP.flow.NO.CalcoloEnergiaImmessaPrelevataNOFlow
import it.eng.au.ERP.flow.TERNA.CalcoloPrelevatoPuntiPrelievoOrariTERNAFlow
import it.eng.au.ERP.flow.CalcoloPrelevatoPuntiPrelievoFULLFlow
import it.eng.au.ERP.flow.INT.CalcoloEnergiaImmessaPrelevataIntFlow
import it.eng.au.ERP.flow.SaldoERPFlow
import it.eng.au.ERP.flow.MessaADisposizioneDATIFlow
import it.eng.au.ERP.utility.args.ERPArgs
import it.eng.au.ERP.utility.environment.Environment
import it.eng.au.ERP.utility.functions.{Constants, argumentsUtilities}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object Main extends Driver{
  @transient  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  override def run(args: Array[String]): Unit = {

    val erpArgs =  ERPArgs.parse(args)

    val flowValue: String = erpArgs.flow.getOrElse(Constants.flowFULL)

    Environment.getOrCreate(s"ERP-Calcolo ${flowValue}", path = erpArgs.pathToProperties)
    val executionId = System.currentTimeMillis()
    val area  =  Environment.getOptionalProperty("spark.app.energia_residuale_pariziale.paramentro.area")
    val annomese  =  Environment.getMandatoryAnnoMese("spark.app.energia_residuale_pariziale.paramentro.annomese")
    val singola_piva_distributore  =  Environment.getOptionalProperty("spark.app.energia_residuale_pariziale.paramentro.singola_piva_distributore")


    implicit val spark: SparkSession =  Environment.getSpark

    val podExcluded = argumentsUtilities.readPodExcluded(erpArgs.path_esclusione_pod)

    flowValue match {
      case Constants.flowDIST =>
        //Execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (DIST)
        // write operation true -> run before TERNA otherwise write need to be set to false
        CalcoloPrelevatoPuntiPrelievoOrariDISTFlow().run(executionId,podExcluded,erpArgs,terna = false,area, annomese, singola_piva_distributore)
      case Constants.flowTERNA =>
        // execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (TERNA)
        // write operation false -> run after DIST otherwise write  need to be set to true
      CalcoloPrelevatoPuntiPrelievoOrariTERNAFlow().run(executionId,podExcluded,erpArgs,terna = true,area, annomese, singola_piva_distributore)
      case Constants.flowIP =>
        // execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (IP)
        CalcoloPrelevatoPuntiPrelievoOrariIPFlow().run(executionId,podExcluded,erpArgs,area, annomese, singola_piva_distributore)
      case Constants.flowINT =>
        // execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (INT)
        CalcoloEnergiaImmessaPrelevataIntFlow().run(executionId,podExcluded,erpArgs,area, annomese, singola_piva_distributore)
      case Constants.flowFULL =>
        // execution CALCOLO PRELEVATO FULL
        CalcoloPrelevatoPuntiPrelievoFULLFlow().run(executionId,podExcluded,erpArgs,area, annomese, singola_piva_distributore)
      case Constants.flowNO =>
        //execution calcolo prelevato ai punti di prelievo no (non orari)
        CalcoloEnergiaImmessaPrelevataNOFlow().run(executionId,podExcluded, annomese)
      case Constants.flowSALDO =>
        // esecuzione saldo ERP (INT - O - NO - IP)
        SaldoERPFlow().run(executionId, annomese)
      case Constants.flowDATI =>
        // messa a disposizione DATI (prima tranche: ERP_AGGREGATO vs DISTRIBUTORE)
        val baseOutput = erpArgs.dati
        MessaADisposizioneDATIFlow().run(executionId, baseOutput, annomese)
    }

//
//        //Execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (DIST)
//        // write operation true -> run before TERNA otherwise write need to be set to false
//        logger.info(s"start flow ${Constants.flowDIST}")
//        CalcoloPrelevatoPuntiPrelievoOrariDISTFlow().run(executionId,podExcluded,erpArgs,terna = false,area, annomese, singola_piva_distributore)
//        logger.info(s"end flow ${Constants.flowDIST}")
//
////         execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (TERNA)
//        // write operation false -> run after DIST otherwise write  need to be set to true
//    logger.info(s"start flow ${Constants.flowTERNA}")
//    CalcoloPrelevatoPuntiPrelievoOrariTERNAFlow().run(executionId,podExcluded,erpArgs,terna = true,area, annomese, singola_piva_distributore)
//    logger.info(s"end flow ${Constants.flowTERNA}")
//
//    // execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (IP)
//    logger.info(s"start flow ${Constants.flowIP}")
//    CalcoloPrelevatoPuntiPrelievoOrariIPFlow().run(executionId,podExcluded,erpArgs,terna = false,area, annomese, singola_piva_distributore)
//    logger.info(s"end flow ${Constants.flowIP}")
//
//    // execution CALCOLO PRELEVATO AI PUNTI DI PRELIEVO ORARI (INT)
//    logger.info(s"start flow ${Constants.flowINT}")
//    CalcoloEnergiaImmessaPrelevataIntFlow().run(executionId,podExcluded,erpArgs)
//    logger.info(s"end flow ${Constants.flowINT}")

  }
  }
