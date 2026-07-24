package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa

import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.RunnableAggregator
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggds.{AggdsDistr, AggdsUdb, AggdsUdd}
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggric._
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.aggric.dettaglioFlussi._
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.dedotti._
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.fin._
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.fin.dettaglioFlussi._
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.pre._
import it.eng.au.aggregatoreConsumiCdp.controller.flowCsvZip.prefin.pre.dettaglioFlussi._
import it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun._
import org.apache.spark.sql.DataFrame

object FactoryCdpDatiPrelievoGas {
  val preAggregators = List(PreDistr, PreUdb, PreUdd, DettaglioFlussiPreDistr, DettaglioFlussiPreUdb, DettaglioFlussiPreUdd)
  val finAggregators = List(FinDistr, FinUdb, FinUdd, DettaglioFlussiFinDistr, DettaglioFlussiFinUdb, DettaglioFlussiFinUdd)
  val ricAggregators = List(AggricDistr, AggricUdb, AggricUdd, DettaglioFlussiRicDistr, DettaglioFlussiRicUdb, DettaglioFlussiRicUdd)
  val dsAggregators = List(AggdsDistr, AggdsUdb, AggdsUdd)
  val dedottiAggregators = List(DedottiDistr, DedottiUdb, DedottiUdd)

  def getCaFinal(aggregators: List[RunnableAggregator]): DataFrame = {
    //run pre
    if (aggregators.forall(preAggregators.contains(_))) CaPre.runCdpDatiPrelievoGas()

    //run fin
    else if (aggregators.forall(finAggregators.contains(_))) CaFin.runCdpDatiPrelievoGas()

    //run aggric
    else if (aggregators.forall(ricAggregators.contains(_))) CaAggric.runCdpDatiPrelievoGas()

    //run aggds
    else if (aggregators.forall(dsAggregators.contains(_))) CaAggds.runCdpDatiPrelievoGas()

    //run dedotti
    else if (aggregators.forall(dedottiAggregators.contains(_))) CaDedotti.runCdpDatiPrelievoGas()

    else
      throw new Exception(s"Unsupported run mode, make sure it's either pre, fin, ric, ds or dedotti.")
  }


}
