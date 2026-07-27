package it.eng.cdp_codprofstd_tds.controller

import it.eng.cdp_codprofstd_tds.controller.Prepare._
import it.eng.cdp_codprofstd_tds.dao.agg._
import it.eng.cdp_codprofstd_tds.schema.CodProfStdDaTdsSchema
import it.eng.cdp_codprofstd_tds.utility.Constants.{DATA_RICEZIONE_FORMAT, DATE_FORMAT}
import it.eng.cdp_codprofstd_tds.utility.Environment
import org.apache.spark.sql.functions.{from_unixtime, lit, unix_timestamp}

object TdsCodProfStd {

  def run(): Unit = {
    val rcuGasMassivoDF = new RcuGasMassivoDao().readPartition(Environment.getRcugasMassivoExecutionId)
    val gasTdsDF = new GasTdsDao().readParquet(Environment.getSettleGasTdsParquetPath)
    val istatRegClima = new IstatRegClimaDao().readTable
    val rcuGasConnessioniDistr2DF = new RcuGasConnessioniDistr2Dao().readTable
    val rcuGasBilanciamentoDF = new RcuGasBilanciamentoDao().readTable
    val prtVsgDF = new PrtVsgDao().readTable
    val prtVtgDF = new PrtVtgDao().readTable
    val prtVsgAggRcuDF = new PrtVsgAggRcuDao().readTable
    val prtVtgAggRcuDF = new PrtVtgAggRcuDao().readTable
    val codProfStdDaTdsDao = new CodProfStdDaTdsDao()

    val annoCompetenza = Environment.getAnnoCompetenza
    val executionId = Environment.executionId
    val freezeDate = Environment.getFreezeDate
    val startDate = from_unixtime(unix_timestamp(lit(Environment.getStartDataRicezione), DATA_RICEZIONE_FORMAT), DATE_FORMAT)
    val endDate = from_unixtime(unix_timestamp(lit(Environment.getEndDataRicezione), DATA_RICEZIONE_FORMAT), DATE_FORMAT)
    val exclusionPdrPath = Environment.getExclusionPdrFilterCsvPath
    val exclusionIsActive = Environment.isExclusionPdrFilterActive.equals("true")
    val forzaturaIsActive = Environment.isFilterPdrForzaturaActive.equals("true")

    val rcuGasMassivo = prepareRcuGasMassivo(rcuGasMassivoDF, freezeDate)
    val rcuGasConnessioniDistr2 = prepareRcuGasConnessioniDistr2(rcuGasConnessioniDistr2DF, freezeDate)
    val rcuGasBilanciamento = prepareRcuGasBilanciamento(rcuGasBilanciamentoDF, freezeDate)
    val gasTds = prepareGasTds(gasTdsDF, startDate, endDate)
    val prtVsg = preparePrtVsg(prtVsgDF, startDate, endDate)
    val prtVtg = preparePrtVtg(prtVtgDF, startDate, endDate)
    val prtVsgAggRcu = preparePrtVsgAggRcu(prtVsgAggRcuDF)
    val prtVtgAggRcu = preparePrtVtgAggRcu(prtVtgAggRcuDF)
    val excludedPdrFromCsv = if (exclusionIsActive) prepareExclusionPdr(exclusionPdrPath) else Environment.getSpark.emptyDataFrame

    val codProfStdDaTds = Transform.transform(rcuGasMassivo, gasTds, istatRegClima, rcuGasConnessioniDistr2, rcuGasBilanciamento, prtVsg, prtVtg, prtVsgAggRcu, prtVtgAggRcu, excludedPdrFromCsv, exclusionIsActive, forzaturaIsActive, freezeDate, annoCompetenza, executionId)
    val outputDataFrame = codProfStdDaTds.selectExpr(CodProfStdDaTdsSchema.getValues: _*)

    codProfStdDaTdsDao.writeParquet(outputDataFrame)
  }


}
