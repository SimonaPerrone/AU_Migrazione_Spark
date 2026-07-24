package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.schema.cig.{PdrCountSchema, PdrTotaleSchema}
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType}

/** Calcola gli indennizzi da sottomettere agli utenti. Per maggiori informazioni, consultare i documenti tecnici. */
object IndennizziController extends Serializable {
  def calcoloIndennizzi(pdrCount: DataFrame): DataFrame = {
    /** Booleano che indica se l'OM 2 è attivo o meno. */
    val isOM2Enabled = Properties.isOM2Enabled //if OM2 is disabled, then OM3 computation is different

    // Lower-bound e upper-bound per l'applicazione degli Obiettivi Minimi 2 e 3 (impostabili da parametro)
    val om2LowerBound = Properties.getOM2LowerBound.toFloat
    val om2UpperBound = Properties.getOM2UpperBound.toFloat
    val om3LowerBound = Properties.getOM3LowerBound.toFloat
    val om3UpperBound = Properties.getOM3UpperBound.toFloat

    // Target da raggiungere affinché un OM sia soddisfatto (impostabili da parametro)
    val om1Target = Properties.getOM1TargetPercent.toFloat
    val om2Target = Properties.getOM2TargetPercent.toFloat
    val om3Target = Properties.getOM3TargetPercent.toFloat

    // Valori degli indenizzi per ciascun PdR al di sotto del target (impostabili da parametro)
    val om1EuroFee = Properties.getOM1EuroFee.toFloat
    val om2EuroFee = Properties.getOM2EuroFee.toFloat
    val om3EuroFee = Properties.getOM3EuroFee.toFloat

      pdrCount
        // Aggiungiamo i valori (lower_bound, upper_bound) per le regole 2 e 3
        .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om2, lit(om2LowerBound).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om2, lit(om2UpperBound).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om3, lit(om3LowerBound).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om3, lit(om3UpperBound).cast(DoubleType))
        // Aggiungiamo i valori percentuali target da raggiungere
        .withColumn(AggregatoTotaleSchema.target_percentage_om1, lit(om1Target).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.target_percentage_om2, lit(om2Target).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.target_percentage_om3, lit(om3Target).cast(DoubleType))
        // Aggiungiamo i valori dell'indennizzo per ogni PdR al di sotto del target
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om1, lit(om1EuroFee).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om2, lit(om2EuroFee).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om3, lit(om3EuroFee).cast(DoubleType))
        // Calcoliamo il numero di PdR target sulla base della percentuale target
        .withColumn(AggregatoTotaleSchema.pdr_target_om1, getTargetPdr(col(PdrCountSchema.pdr_g), col(AggregatoTotaleSchema.target_percentage_om1)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.pdr_target_om2, getTargetPdr(col(PdrCountSchema.pdr_g_om1), col(AggregatoTotaleSchema.target_percentage_om2)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.pdr_target_om3, getTargetPdr(col(PdrCountSchema.pdr_g_om1), col(AggregatoTotaleSchema.target_percentage_om3)).cast(DoubleType))

        .withColumn("is_om2_enabled", lit(isOM2Enabled).cast(BooleanType))
        // Calcoliamo le percentuali raggiunte tramite le 3 formule e il delta dei PdR (negativo o positivo) per ognuna dei 3 Obiettivi Minimi
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om1, computeOM1(col(PdrCountSchema.pdr_g), col(PdrCountSchema.pdr_g_om1)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om1, getDeltaPdr(col(PdrCountSchema.pdr_g_om1), col(AggregatoTotaleSchema.pdr_target_om1)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om2, computeOM2(col(PdrCountSchema.pdr_g_om1), col(PdrCountSchema.pdr_g_om2)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om2, getDeltaPdr(col(PdrCountSchema.pdr_g_om2), col(AggregatoTotaleSchema.pdr_target_om2)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om3, computeOM3(col(PdrCountSchema.pdr_g_om1), col(PdrCountSchema.pdr_g_om3), col(AggregatoTotaleSchema.delta_pdr_om2), col("is_om2_enabled")).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om3, getDeltaPdrForOM3(col(PdrCountSchema.pdr_g_om3), col(AggregatoTotaleSchema.pdr_target_om3), col(AggregatoTotaleSchema.delta_pdr_om2)).cast(DoubleType))
        // Utilizzando il delta, calcoliamo l'indennizzo da sottomettere, se positivo
        .withColumn(AggregatoTotaleSchema.indennizzo_om1, getIndennizzo(col(AggregatoTotaleSchema.euro_fee_per_pdr_om1), col(AggregatoTotaleSchema.delta_pdr_om1)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.indennizzo_om2, getIndennizzo(col(AggregatoTotaleSchema.euro_fee_per_pdr_om2), col(AggregatoTotaleSchema.delta_pdr_om2)).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.indennizzo_om3, getIndennizzo(col(AggregatoTotaleSchema.euro_fee_per_pdr_om3), col(AggregatoTotaleSchema.delta_pdr_om3)).cast(DoubleType))

        .withColumn(AggregatoTotaleSchema.executionid, lit(Environment.executionId).cast(LongType))
      .withColumn(AggregatoTotaleSchema.id_indennizzo, hash(col(PdrTotaleSchema.piva_udd), col(PdrTotaleSchema.piva_distr), col(PdrTotaleSchema.executionid)).cast(LongType) + Int.MaxValue)
      .withColumn(AggregatoTotaleSchema.annomese, lit(Properties.getYearMonth))
      .selectExpr(AggregatoTotaleSchema.getValues: _*)
  }

  /** Ottiene il numero target di PdR, ottenuto come (# totale dei PdR * target percentuale) */
  private def getTargetPdr: UserDefinedFunction = udf((pdrCount: Int, percent: Float) => (percent * pdrCount / 100).round)

  /** Ottiene il delta dei PdR, ottenuto come (# dei PdR - # target dei PdR) */
  private def getDeltaPdr: UserDefinedFunction = udf((pdrCount: Int, pdrTarget: Int) => pdrCount - pdrTarget)

  /** Calcola il deta per l'Obiettivo Minimo 3, poiché è richiesto anche il delta dell'OM2. */
  //deltaPdrOM2.max(0) gestisce entrambi i casi per il calcolo di deltaPdrOM3
  private def getDeltaPdrForOM3: UserDefinedFunction = udf((pdrCount: Int, pdrTarget: Int, deltaPdrOM2: Int) => pdrCount + deltaPdrOM2.max(0) - pdrTarget)

  /** Calcola l'indennizzo da sottomettere */
  //Se deltaPdr<0, allora vi è un indennizzo, altrimenti l'indennizzo è 0
  private def getIndennizzo: UserDefinedFunction = udf((euro: Float, deltaPdr: Int) => (euro * deltaPdr.min(0)).abs)

  /** Calcola la percentuale di che soddisfa l'Obiettivo Minimo 1. */
  private def computeOM1: UserDefinedFunction = udf((pdrG: Int, pdrOM1: Int) => {
    val om1 = if (pdrG != 0) pdrOM1.toFloat / pdrG * 100
    else 100

    om1.round
  })

  /** Calcola la percentuale di che soddisfa l'Obiettivo Minimo 2. */
  private def computeOM2: UserDefinedFunction = udf((pdrOM1: Int, pdrOM2: Int) => {
    val om2 = if (pdrOM1 != 0) pdrOM2.toFloat / pdrOM1 * 100
    else 100

    om2.round
  })

  /** Calcola la percentuale di che soddisfa l'Obiettivo Minimo 1. */
  private def computeOM3: UserDefinedFunction = udf((pdrOM1: Int, pdrOM3: Int, deltaPdrOM2: Float, isOM2Enabled: Boolean) => {
    // Case 1: OM2>75%, then deltaPdrOM2.max(0)=deltaPdrOM2
    // Case 2: OM2<=75%, then deltaPdrOM2.max(0)=0
    val deltaPdrOM2forOM3 = if (isOM2Enabled) deltaPdrOM2 else 0

    val om3 = if (pdrOM1 != 0) (pdrOM3 + deltaPdrOM2forOM3.max(0)) / pdrOM1 * 100
    else 100

    om3.round
  })

  /** Forza a null i valori delle regole disabilitate. */
  def forceNulls(aggregatoTotale: DataFrame): DataFrame = {
    val isOM1Enabled = Properties.isOM1Enabled
    val isOM2Enabled = Properties.isOM2Enabled
    val isOM3Enabled = Properties.isOM3Enabled

    val aggregatoWithOM1Disabled = if (!isOM1Enabled)
      aggregatoTotale
        .withColumn(AggregatoTotaleSchema.pdr_g_om1, lit(null).cast(LongType))
        .withColumn(AggregatoTotaleSchema.target_percentage_om1, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om1, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.pdr_target_om1, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om1, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om1, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.indennizzo_om1, lit(null).cast(DoubleType))
    else aggregatoTotale

    val aggregatoWithOM2Disabled = if (!isOM2Enabled)
      aggregatoWithOM1Disabled
        .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om2, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om2, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.pdr_g_om2, lit(null).cast(LongType))
        .withColumn(AggregatoTotaleSchema.target_percentage_om2, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om2, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.pdr_target_om2, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om2, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om2, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.indennizzo_om2, lit(null).cast(DoubleType))
    else aggregatoWithOM1Disabled

    val aggregatoWithOM3Disabled = if (!isOM3Enabled)
      aggregatoWithOM2Disabled
        .withColumn(AggregatoTotaleSchema.percentage_lower_bound_om3, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.percentage_upper_bound_om3, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.pdr_g_om3, lit(null).cast(LongType))
        .withColumn(AggregatoTotaleSchema.target_percentage_om3, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.achieved_percentage_om3, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.pdr_target_om3, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.delta_pdr_om3, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.euro_fee_per_pdr_om3, lit(null).cast(DoubleType))
        .withColumn(AggregatoTotaleSchema.indennizzo_om3, lit(null).cast(DoubleType))
    else aggregatoWithOM2Disabled

    aggregatoWithOM3Disabled
  }
}
