package it.sferanet.au.dal.output

import it.sferanet.au.model.{Consumption, ConsumptionOutput}
import it.sferanet.au.schema.ConsumptionSchema
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Timestamp

class ConsumptionTable(session: String, executionId: Long) extends Serializable {

  private val outputPath = Environment.getConsumptionPath

  def createDataFrame(consumptions: RDD[Consumption]): DataFrame = {
    val consumptionDF = Environment.getSqlContext.createDataFrame(
      consumptions.map(c =>

        ConsumptionOutput(
          pdr = c.pdr,
          startservice = c.startService,
          endservice = c.endService,
          startsegment = new Timestamp(c.startSegment.getTime),
          endsegment = new Timestamp(c.endSegment.getTime),
          startvalue = c.startvalue,
          endvalue = c.endvalue,
          idconsumptionerrorstate = c.idConsumptionErrorState.id,
          n_coeff_correzione = c.nCoeffCorrezione,
          t_misuratore_integrato = c.startTMisuratoreIntegrato,
          end_t_misuratore_integrato = c.endTTMisuratoreIntegrato,
          t_pre_conv = c.startTPreConv,
          end_t_pre_conv = c.endTPreConv,
          t_cod_prof = None, //c.startTCodProf,
          n_prelievo_annuo = None, //c.nPrelievoAnnuo,
          tipo_coeff = c.tipoCoeff,
          tipo_forzatura = c.tipoForzatura,
          coerenza_dim = c.coerenzaDim,
          session = session,
          executionid = executionId
        )))

    consumptionDF
  }

  def write(df: DataFrame): Unit = {
    df
      .selectExpr(ConsumptionSchema.getValues: _*)
      .write
      .partitionBy(ConsumptionSchema.session, ConsumptionSchema.executionid)
      .mode(SaveMode.Append)
      .parquet(outputPath)

    if(!Environment.isLocalMode) Environment.getSpark.sql(s"MSCK REPAIR TABLE ${Environment.getConsumptionTable}")
  }
}
