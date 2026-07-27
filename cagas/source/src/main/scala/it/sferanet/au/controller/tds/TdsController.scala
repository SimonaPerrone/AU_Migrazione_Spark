package it.sferanet.au.controller.tds

import it.sferanet.au.dal.TdsTable
import it.sferanet.au.model.Tds
import it.sferanet.au.schema._
import it.sferanet.au.utilities.{Constants, Environment}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.util.Date

object TdsController {
  val DATA_RICEZIONE_FORMAT = "yyyy/MM/dd"
  val TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
  val data_filtro = "data_filtro"

  /**
   * Legge la tabella gas_tds, filtrando i record come segue: vengono mantenuti i record per cui la data_creazione è minore o uguale alla tdsEndDate
   * (parametro presente nel file di configurazione). Se isForDedotti è true, allora si controlla anche che la data_creazione sia maggiore o uguale alla tdsLastUpdatedDate (parametro nel file di configurazione).
   *  Questo perché, nel caso dei dedotti, vogliamo intercettare i PdR per cui c'è stato un aggiornamento sulla gas_tds ma non è stato intercettato dalla procedura che
   *  popola la cod_prof_std_da_tds (quindi tdsLastUpdatedDate non è altro che la data di ultima modifica della cod_prof_std_da_tds).
   * @param settleGasTdsPath tabella gas_tds in cui sono contenute le informazioni più recenti di classe_prelievo e cat_uso
   * @param tdsEndDate estremo destro di lettura della gas_tds
   * @param isForDedotti true se occorre leggere la gas_tds applicando il filtro sui dedotti
   * @param tdsLastUpdatedDate utilizzato nel caso [[isForDedotti]] sia true come estremo sinistro per la data_creazione
   * @return un RDD di PdR con la Tds associata
   */
  def getTdsTable(settleGasTdsPath: String, tdsEndDate: Date, isForDedotti: Boolean = false, tdsLastUpdatedDate: Date): RDD[(String, Tds)] = {
    new TdsTable(settleGasTdsPath).get()
      .filter(v => v.data_creazione.before(tdsEndDate) || v.data_creazione == tdsEndDate)
      // we apply the filter if we need to get the tds for dedotti (i.e. we select only the pdrs which have been updated after last ds update)
      .filter(v => !isForDedotti || v.data_creazione.after(tdsLastUpdatedDate) || v.data_creazione == tdsLastUpdatedDate)
      .map(v => (v.pdr, v)).groupByKey()
      .map({ case (pdr, values) => (pdr, values.maxBy(_.data_creazione)) })
  }

  /** Filtra la tds rimuovendo i PdR ottenuti dalle tabelle prt_vtg e prt_vsg. Per maggiori informazioni, consultare i documenti tecnici. */
  def filterTds(tdsTable: RDD[(String, Tds)], pdrsToRemove: RDD[(String, Date)]): RDD[(String, Tds)] = {
    tdsTable
      .leftOuterJoin(pdrsToRemove)
      .filter({ case (_, (tds, vsgMatchDate)) => vsgMatchDate.isEmpty || vsgMatchDate.get.before(tds.data_creazione) })
      .map({ case (pdr, (tds, _)) => (pdr, tds) })
  }

  def convertToDF(tds: RDD[(String, Tds)]): DataFrame = {
    val sqlContext = Environment.getSqlContext
    import sqlContext.implicits._

    tds
      .map({ case (pdr, tds) => (pdr, tds.cat_uso, tds.classe_prelievo) })
      .toDF(SettleGasGasTdsSchema.cod_pdr, SettleGasGasTdsSchema.cat_uso, SettleGasGasTdsSchema.classe_prelievo)
  }

  def convertToDFForPresTds(tds: RDD[(String, Tds)]): DataFrame = {
    val sqlContext = Environment.getSqlContext
    import sqlContext.implicits._

    tds
      .map({ case (pdr, _) => pdr })
      .toDF(SettleGasGasTdsSchema.cod_pdr)
  }

  /** Ottiene la lista di PdR da rimuovere dalla gas_tds, secondo le logiche richieste da AU. */
  def getPdrsToRemove(readVsg: DataFrame, readVtg: DataFrame, readVsgAggRcu: DataFrame, readVtgAggRcu: DataFrame, tdsEndDate: String): DataFrame = {
    val prtVsg = preparePrtVsg(readVsg, tdsEndDate)
    val prtVtg = preparePrtVtg(readVtg, tdsEndDate)
    val prtVsgAggRcu = preparePrtVsgAggRcu(readVsgAggRcu)
    val prtVtgAggRcu = preparePrtVtgAggRcu(readVtgAggRcu)

    val pdrVsg = prtVsg.join(prtVsgAggRcu, prtVsg(PrtVsgSchema.n_id_pratica) === prtVsgAggRcu(PrtVsgAggRcuSchema.n_id_pratica))
      .select(PrtVsgSchema.t_codice_pdr, data_filtro)
      .distinct()

    val pdrVtg = prtVtg.join(prtVtgAggRcu, prtVtg(PrtVtgSchema.n_id_pratica) === prtVtgAggRcu(PrtVtgAggRcuSchema.n_id_pratica))
      .select(PrtVtgSchema.t_codice_pdr, data_filtro)
      .distinct()

    val pdrsToRemove = pdrVsg
      .union(pdrVtg)
      .coalesce(pdrVsg.rdd.getNumPartitions)
      .distinct()
      .groupBy(PrtVsgSchema.t_codice_pdr)
      .agg(max(data_filtro).as(data_filtro))

    pdrsToRemove
  }

  def getPdrsToRemoveRDD(df: DataFrame): Option[RDD[(String, Date)]] = {
    val rdd =
      df
        .rdd
        .map(row => (row.getAs[String](PrtVsgSchema.t_codice_pdr),
        Constants.getDate(Constants.getFormatter(TIMESTAMP_FORMAT), row.getAs[String](data_filtro)).getOrElse(new Date(0))))

    Some(rdd)
  }

  def preparePrtVsg(df: DataFrame, tdsEndDate: String): DataFrame = {
    val endDate = from_unixtime(unix_timestamp(lit(tdsEndDate), "yyyy-MM-dd"), TIMESTAMP_FORMAT)

    df
      .withColumn(PrtVsgSchema.d_data_esecuzione, date_format(col(PrtVsgSchema.d_data_esecuzione), TIMESTAMP_FORMAT))
      .filter(col(PrtVsgSchema.t_stato) === "F"
        && col(PrtVsgSchema.t_tipo_prestazione).isin("A40", "A01")
        && col(PrtVsgSchema.d_data_esecuzione) <= endDate)
      .withColumnRenamed(PrtVsgSchema.d_data_esecuzione, data_filtro)
      .select(PrtVsgSchema.t_codice_pdr, PrtVsgSchema.n_id_pratica, data_filtro)
  }

  def preparePrtVtg(df: DataFrame, tdsEndDate: String): DataFrame = {
    val endDate = from_unixtime(unix_timestamp(lit(tdsEndDate), "yyyy-MM-dd"), TIMESTAMP_FORMAT)

    df
      .withColumn(PrtVtgSchema.d_data_dec, date_format(col(PrtVtgSchema.d_data_dec), TIMESTAMP_FORMAT))
      .filter(col(PrtVtgSchema.t_stato).isin("F3", "F4") && col(PrtVtgSchema.d_data_dec) <= endDate)
      .withColumnRenamed(PrtVtgSchema.d_data_dec, data_filtro)
      .select(PrtVtgSchema.t_codice_pdr, PrtVtgSchema.n_id_pratica, data_filtro)
  }

  def preparePrtVtgAggRcu(df: DataFrame): DataFrame = {
    df
      .filter(col(PrtVtgAggRcuSchema.t_esito_agg_rcu) === "1")
      .select(PrtVtgAggRcuSchema.n_id_pratica)
  }

  def preparePrtVsgAggRcu(df: DataFrame): DataFrame = {
    df
      .filter(col(PrtVsgAggRcuSchema.t_esito_agg_rcu) === "1")
      .select(PrtVsgAggRcuSchema.n_id_pratica)
  }
}
