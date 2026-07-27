package it.eng.au.ERP.flow

import it.eng.au.ERP.Main.logger
import it.eng.au.ERP.flow.DIST.CalcoloPrelevatoPuntiPrelievoOrariDISTFlow
import it.eng.au.ERP.flow.INT.CalcoloEnergiaImmessaPrelevataIntFlow
import it.eng.au.ERP.flow.IP.CalcoloPrelevatoPuntiPrelievoOrariIPFlow
import it.eng.au.ERP.flow.NO.CalcoloEnergiaImmessaPrelevataNOFlow
import it.eng.au.ERP.flow.TERNA.CalcoloPrelevatoPuntiPrelievoOrariTERNAFlow
import it.eng.au.ERP.utility.args.ERPArgsConfig
import it.eng.au.ERP.utility.functions.Constants
import org.apache.spark.sql.SparkSession

import java.util.concurrent.Executors
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

case class CalcoloPrelevatoPuntiPrelievoFULLFlow()(implicit spark: SparkSession) extends Flow {

  def run(
      executionId: Long,
      podExcluded: List[String],
      erpArgs: ERPArgsConfig,
      area: Option[String],
      annomese: Option[String],
      singola_piva_distributore: Option[String]
  ): Unit = {

    // Parallel orchestration strategy:
    // - Run INT, IP, NO in parallel
    // - Run DIST and TERNA sequentially within the same future to avoid concurrent writes on the same O table
    // - After all complete successfully, run SALDO

    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

    try {
      val distTernaF: Future[Unit] = Future {
        logger.info(s"start flow ${Constants.flowDIST}")
        CalcoloPrelevatoPuntiPrelievoOrariDISTFlow()
          .run(executionId, podExcluded, erpArgs, terna = false, area, annomese, singola_piva_distributore)
        logger.info(s"end flow ${Constants.flowDIST}")

        logger.info(s"start flow ${Constants.flowTERNA}")
        CalcoloPrelevatoPuntiPrelievoOrariTERNAFlow()
          .run(executionId, podExcluded, erpArgs, terna = true, area, annomese, singola_piva_distributore)
        logger.info(s"end flow ${Constants.flowTERNA}")
      }

      val ipF: Future[Unit] = Future {
        logger.info(s"start flow ${Constants.flowIP}")
        CalcoloPrelevatoPuntiPrelievoOrariIPFlow()
          .run(executionId, podExcluded, erpArgs, area, annomese, singola_piva_distributore)
        logger.info(s"end flow ${Constants.flowIP}")
      }

      val intF: Future[Unit] = Future {
        logger.info(s"start flow ${Constants.flowINT}")
        CalcoloEnergiaImmessaPrelevataIntFlow()
          .run(executionId, podExcluded, erpArgs, area, annomese, singola_piva_distributore)
        logger.info(s"end flow ${Constants.flowINT}")
      }

      // Wait until DIST/TERNA, IP, INT complete
      val preAll = Future.sequence(Seq(distTernaF, ipF, intF))
      Await.result(preAll, Duration.Inf)

      // Run NO serially after the others
      logger.info(s"start flow ${Constants.flowNO}")
      CalcoloEnergiaImmessaPrelevataNOFlow().run(executionId, podExcluded, annomese)
      logger.info(s"end flow ${Constants.flowNO}")

      // After successful completion, compute SALDO using latest executionids
      logger.info(s"start flow ${Constants.flowSALDO}")
      SaldoERPFlow().run(executionId, annomese)
      logger.info(s"end flow ${Constants.flowSALDO}")

    } finally {
      // shutdown the thread pool
      ec.asInstanceOf[java.util.concurrent.ExecutorService].shutdown()
    }
  }
}
