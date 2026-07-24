package it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.flowRun

import it.eng.au.aggregatoreConsumiCdp.controller.flowPrepareCa.FlowCdpDatiPrelievoGas
import it.eng.au.aggregatoreConsumiCdp.dao.cdp.{CaDao, MassivoFreezeDao}
import it.eng.au.aggregatoreConsumiCdp.schema.{CaSchema, MassivoFreezeSchema, OutputHiveSchema}
import it.eng.au.aggregatoreConsumiCdp.utility.Constants.{DATA_DECORRENZA_FORMAT, TIMESTAMP_FORMAT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CaAggric extends FlowCdpDatiPrelievoGas {
  override def addPiva(caFinal: DataFrame, distributore: DataFrame, azienda: DataFrame, udb: DataFrame): DataFrame = {
    caFinal
  }

  override def specificTransform(caFinal: DataFrame): DataFrame = {
    val caExecutionId = getCaExecutionId
    val ca = new CaDao().readPartition(caExecutionId)
    val caFinalFiltered = removeDedotti(caFinal, ca)

    val (dataDecorrenza, execIdMassivo) = getDataDecorrenzaExecIdMassivo(caFinalFiltered)
    val massivoFreezer = new MassivoFreezeDao().readPartition(execIdMassivo)

    val recoverSwitch = recoverUddSwitching(caFinalFiltered, massivoFreezer, dataDecorrenza + "-10-01", execIdMassivo)
    recoverSwitch
  }

  def getDataDecorrenzaExecIdMassivo(caFinal: DataFrame): (String, Long) = {
    val (dataDecorrenza, execIdMassivo) = caFinal
      .select(col(OutputHiveSchema.anno_competenza), col(OutputHiveSchema.massivo_freezer_executiond_id))
      .filter(col(OutputHiveSchema.anno_competenza).isNotNull && (col(OutputHiveSchema.anno_competenza) =!= ""))
      .take(1)
      .map(row =>
        (
          row.getAs[String](OutputHiveSchema.anno_competenza).toInt - 1,
          row.getAs[Long](OutputHiveSchema.massivo_freezer_executiond_id)
        )
      )
      .toList.head
    //convertLocalDateTimeToStringWithFormat(convertStringTimestampToLocalDateTime(getCurrentThermalYear), DATA_DECORRENZA_FORMAT)

    (dataDecorrenza.toString, execIdMassivo)
  }

  def recoverUddSwitching(df: DataFrame, massivoFreezer: DataFrame, dataDecorrenza: String, execIdMassivo: Long): DataFrame = {
    val caFinal = df
      .withColumn(OutputHiveSchema.data_decorrenza, from_unixtime(unix_timestamp(concat(lit("01/10/"), col(OutputHiveSchema.anno_competenza) - 1), DATA_DECORRENZA_FORMAT), TIMESTAMP_FORMAT))

    val massivoFreezerGetSwithing = massivoFreezer
      .filter(upper(col(MassivoFreezeSchema.t_processo)).isin("SWG", "UIG") and col(MassivoFreezeSchema.n_id_fornitura).isNotNull and col(MassivoFreezeSchema.data_fine_for) > dataDecorrenza)
      .selectExpr(MassivoFreezeSchema.t_codice_pdr, MassivoFreezeSchema.piva_udd)
      .groupBy(col(MassivoFreezeSchema.t_codice_pdr))
      .agg(collect_set(col(MassivoFreezeSchema.piva_udd)).as(OutputHiveSchema.udd_oggetto_swithcing))
    // TODO - DA METTERE array_except PER TOGLIERE LA piva_udd IN udd_oggetto_swithcing CON SPARK >2.4.0 E RIMUOVERE LA DISTINCT DA AggRicUdd.scala riga 44
    caFinal
      .join(massivoFreezerGetSwithing, caFinal(OutputHiveSchema.cod_pdr) === massivoFreezerGetSwithing(MassivoFreezeSchema.t_codice_pdr), "left")
      .drop(massivoFreezerGetSwithing(MassivoFreezeSchema.t_codice_pdr))
      .withColumn(OutputHiveSchema.udd_oggetto_swithcing, coalesce(concat_ws(";", col(OutputHiveSchema.udd_oggetto_swithcing)), lit("")))
      .withColumn(OutputHiveSchema.massivo_freezer_executiond_id, lit(execIdMassivo))
      .selectExpr(OutputHiveSchema.getValues: _*)

  }

  def removeDedotti(caFinal: DataFrame, ca: DataFrame): DataFrame = {
    val caPrepare = ca
      .filter(col(CaSchema.idcaerrorcode) === 0)
      .select(CaSchema.pdr)
      .distinct()

    caFinal
      .join(caPrepare, caFinal(OutputHiveSchema.cod_pdr) === caPrepare(CaSchema.pdr))
      .drop(caPrepare(CaSchema.pdr))
  }
}
