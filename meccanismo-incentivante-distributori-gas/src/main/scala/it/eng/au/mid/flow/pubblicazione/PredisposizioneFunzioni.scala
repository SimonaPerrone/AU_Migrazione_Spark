package it.eng.au.mid.flow.pubblicazione

import it.eng.au.mid.common.CostantiMid
import it.eng.au.mid.environment.Environment
import it.eng.au.mid.model.file.pubblicazione.MidAlphaValoriModel
import it.eng.au.mid.model.flow.DailyConsumptionModel
import it.eng.au.mid.model.hive.mid.{Mid1DettaglioModel, MidContatoriModel}
import it.eng.au.mid.schema.file.pubblicazione.MidAlphaValoriSchema
import it.eng.au.mid.schema.flow.calcolo.DailyConsumptionSchema
import it.eng.au.mid.schema.hive.mid.{Mid1DettaglioSchema, MidContatoriSchema}
import org.apache.spark.sql.functions.{col, lit, max, not}
import org.apache.spark.sql.{Dataset, SparkSession}

/** *
 * Classe di funzioni di predisposizione in comune tra MID1 e MID2
 */
object PredisposizioneFunzioni {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  /**
   * Filtra contatori mid prendendo per ogni mese nell'intervallo definito i dati relativi all'ultimo executionId
   * processo_tracciatura: AGG
   * stato: Valido o forzato
   * contatore N: maggiore di soglia
   */
  def filtraMidContatoriDaAnalizzare(midContatoriDs: Dataset[MidContatoriModel], annomeseDa: String, annomeseA: String,
                                     sogliaContatore: Int): Dataset[MidContatoriModel] = {
    // df con coppia annomese - max execution ID usato per filtrare midContatori
    val maxExecIdAnnomese = midContatoriDs
      .groupBy(MidContatoriSchema.annomese)
      .agg(
        max(MidContatoriSchema.executionid_tracciatura) as MidContatoriSchema.executionid_tracciatura
      ).cache()

    midContatoriDs
      .where(col(MidContatoriSchema.annomese) >= annomeseDa)
      .where(col(MidContatoriSchema.annomese) <= annomeseA)
      .where(col(MidContatoriSchema.stato).isin(CostantiMid.STATO_VALIDO, CostantiMid.STATO_FORZATO))
      .where(col(MidContatoriSchema.contatore) > sogliaContatore)
      .join(maxExecIdAnnomese,
        midContatoriDs(MidContatoriSchema.annomese) === maxExecIdAnnomese(MidContatoriSchema.annomese)
          and midContatoriDs(MidContatoriSchema.executionid_tracciatura) === maxExecIdAnnomese(MidContatoriSchema.executionid_tracciatura),
        "LEFT_SEMI"
      )
      .selectExpr(MidContatoriSchema.getValues: _*)
      .as[MidContatoriModel]
  }

  /** *
   * Rimuovi da midContatori elementi che hanno nella colonna colonna i valori passati
   */
  def escludiElementi(midContatori: Dataset[MidContatoriModel], colonna: String, valori: List[String]): Dataset[MidContatoriModel] = {
    if (valori.isEmpty) {
      midContatori
    } else {
      midContatori.where(not(col(colonna).isin(valori: _*))) // NOT
    }
  }

  /** *
   * Filtra dailyConsumption per leggere solo mesi interessati dal calcolo selezionando mesi ed executionid da midContatori filtrati
   */
  def filtraDailyConsumption(dailyConsumptionAggDs: Dataset[DailyConsumptionModel],
                             midContatori: Dataset[MidContatoriModel]): Dataset[DailyConsumptionModel] = {
    val annomeseExecutionIdValidi = midContatori
      .select(MidContatoriSchema.annomese, MidContatoriSchema.executionid_daily_consumption)
      .dropDuplicates()

    dailyConsumptionAggDs.join(
        annomeseExecutionIdValidi,
        dailyConsumptionAggDs(DailyConsumptionSchema.annomese) === annomeseExecutionIdValidi(MidContatoriSchema.annomese)
          and dailyConsumptionAggDs(DailyConsumptionSchema.executionid) === annomeseExecutionIdValidi(MidContatoriSchema.executionid_daily_consumption),
        "LEFT_SEMI")
      .selectExpr(DailyConsumptionSchema.getValues: _*)
      .as[DailyConsumptionModel]
  }

  /** *
   * Completa l'informazione finale con i dati dalla dailyConsumption
   */
  def aggiungiInformazioniAnagrafica(midContatoriDs: Dataset[MidContatoriModel],
                                     dailyConsumptionAggDs: Dataset[DailyConsumptionModel],
                                     executionId: Long
                                    ): Dataset[Mid1DettaglioModel] = {
    // mantieni solo info valide e senza duplicati
    val dailyConsumptionFiltrato = dailyConsumptionAggDs
      .select(
        DailyConsumptionSchema.pdr,
        DailyConsumptionSchema.annomese,
        DailyConsumptionSchema.pivadistr,
        DailyConsumptionSchema.pivaudd,
        DailyConsumptionSchema.codremi,
        DailyConsumptionSchema.classemisuratore
      )
      .where(col(DailyConsumptionSchema.pivadistr).isNotNull)
      .where(col(DailyConsumptionSchema.pivaudd).isNotNull)
      .where(col(DailyConsumptionSchema.codremi).isNotNull)
      .where(col(DailyConsumptionSchema.classemisuratore).isNotNull)
      .dropDuplicates()

    // completa con le info richieste
    midContatoriDs
      .join(dailyConsumptionFiltrato,
        midContatoriDs(MidContatoriSchema.pdr) === dailyConsumptionAggDs(DailyConsumptionSchema.pdr)
          and midContatoriDs(MidContatoriSchema.annomese) === dailyConsumptionAggDs(DailyConsumptionSchema.annomese),
        "INNER"
      )
      .select(
        midContatoriDs(MidContatoriSchema.pdr) as Mid1DettaglioSchema.pdr,
        midContatoriDs(MidContatoriSchema.contatore) as Mid1DettaglioSchema.contatore,
        dailyConsumptionAggDs(DailyConsumptionSchema.pivadistr) as Mid1DettaglioSchema.piva_id,
        dailyConsumptionAggDs(DailyConsumptionSchema.pivaudd) as Mid1DettaglioSchema.piva_udd,
        dailyConsumptionAggDs(DailyConsumptionSchema.codremi) as Mid1DettaglioSchema.cod_remi,
        dailyConsumptionAggDs(DailyConsumptionSchema.classemisuratore) as Mid1DettaglioSchema.gdm,
        lit(0) as Mid1DettaglioSchema.alpha, // calcolato successivamente
        midContatoriDs(MidContatoriSchema.executionid_tracciatura) as Mid1DettaglioSchema.executionid_mid_contatori,
        midContatoriDs(MidContatoriSchema.annomese) as Mid1DettaglioSchema.annomese,
        lit(executionId) as Mid1DettaglioSchema.executionid
      )
      .as[Mid1DettaglioModel]
  }

  /**
   * Associa valore alpha a gdm; se alpha nullo allora escludi
   */
  def calcolaAlpha(midContatoriArricchiti: Dataset[Mid1DettaglioModel], alphaDs: Dataset[MidAlphaValoriModel]): Dataset[Mid1DettaglioModel] = {
    midContatoriArricchiti.join(
        alphaDs,
        midContatoriArricchiti(Mid1DettaglioSchema.gdm) === alphaDs(MidAlphaValoriSchema.gdm),
        "LEFT"
      )
      .drop(midContatoriArricchiti(Mid1DettaglioSchema.alpha))
      .drop(alphaDs(MidAlphaValoriSchema.gdm))
      .where(alphaDs(MidAlphaValoriSchema.alpha).isNotNull)
      .selectExpr(Mid1DettaglioSchema.getValues: _*)
      .as[Mid1DettaglioModel]
  }

  /** *
   * Rimuovi da midContatori i distributori da escludere
   */
  def escludiDistributori(midDettaglio: Dataset[Mid1DettaglioModel], valori: List[String]): Dataset[Mid1DettaglioModel] = {
    if (valori.isEmpty) {
      midDettaglio
    } else {
      midDettaglio.where(not(col(Mid1DettaglioSchema.piva_id).isin(valori: _*))) // NOT
    }
  }

}
