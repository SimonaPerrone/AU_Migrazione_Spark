package it.eng.au.portale_consumi_ee.trasformation

import it.eng.au.portale_consumi_ee.Main.logger
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.common.utility.functions.{costants, sqlToSparkUtilitties}
import it.eng.au.portale_consumi_ee.model.mongodbs.{FornitureInfoModel, FornitureModel}
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuFornituraPModel, RcuPodPModel, RcuPodStatoPModel, RcuResidenzaPModel, RcuTariffaPModel}
import it.eng.au.portale_consumi_ee.model.rcus.{RcusFornituraPModel, RcusPodstatoPModel}
import it.eng.au.portale_consumi_ee.schema.mongodbs.{FornitureInfoSchema, FornitureSchema}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuFornituraPSchema, RcuPodPSchema, RcuPodStatoPSchema, RcuResidenzaPSchema, RcuTariffaPSchema}
import it.eng.au.portale_consumi_ee.schema.rcus.{RcusFornituraPSchema, RcusPodstatoPSchema}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

// spark implementation of hql_forniture_ele_0_human_readble.sql
object forniture_ele_0_trasformations {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  val MAX_DATA_AGGIORNAMENTO = "max_d_aggiornamento"
  //dataframe names
  val pods = "pods"
  val forn = "forn"
  val stato_pods = "stato_pods"

  val azienda = "azienda"
  val forniture = "forniture"
  val tariffa = "tariffa"
  val residenza = "residenza"

  //column names
  val n_id_fornitura_numeric = "n_id_fornitura_numeric"
  val d_inizio = "d_inizio"
  val d_fine = "d_fine"
  val codice_pod = "codice_pod"
  val attivo = "attivo"
  val n_id_fornitore = "n_id_fornitore"
  val t_servizio_tutela_sii = "t_servizio_tutela_sii"
  val d_fine_str= "d_fine_str"
  val n_id_pod= "n_id_pod"
  val n_id_cliente= "n_id_cliente"
  val t_tipo_mercato= "t_tipo_mercato"
  val n_id_indirizzo= "n_id_indirizzo"
  val n_id_ind_forn= "n_id_ind_forn"
  val d_inizio_start= "d_inizio_start"
  val n_id_fornitura = "n_id_fornitura"
  val d_inizio_str = "d_inizio_str"
  val inizio = "inizio"
  val fine = "fine"
  val min_inizio = "min_inizio"
  val n_id_azienda_numeric = "n_id_azienda_numeric"

  val max_fine= "max_fine"
  val k_key = "k_key"

  def calcolo_forniture(dsFornitura: Dataset[RcuFornituraPModel],
                        dsPod:Dataset[RcuPodPModel],
                        dsPodStato:Dataset[RcuPodStatoPModel],
                        dsRcusFornitura :  Dataset[RcusFornituraPModel],
                        dsRcusPodStato:Dataset[RcusPodstatoPModel]
                    ): Dataset[FornitureModel] = {

    //FIRST PART TBL_DATA

    // Filter rows based on the condition T_STATO_ATTIVAZIONE = 'A' OR NVL(T_STATO_ATTIVAZIONE, '') = ''
    def  dsPodStatoPrepared = dsPodStato
      .filter(col(RcuPodStatoPSchema.t_stato_attivazione).equalTo("A") ||
        col(RcuPodStatoPSchema.t_stato_attivazione).isNull ||
        col(RcuPodStatoPSchema.t_stato_attivazione).equalTo(""))

//    val countdsPodStato = dsPodStato.count().toString
//    println(s"countdsPodStato is: $countdsPodStato")

    val dfPodStatoAggregated = dsPodStatoPrepared
      .groupBy(col(RcuPodStatoPSchema.n_id_pod),col(RcuPodStatoPSchema.t_stato_attivazione))
      .agg(max(RcuPodStatoPSchema.d_aggiornamento).alias(MAX_DATA_AGGIORNAMENTO))
      .select(col(RcuPodStatoPSchema.n_id_pod),
              col(MAX_DATA_AGGIORNAMENTO),
              col(RcuPodStatoPSchema.t_stato_attivazione)
      ).persist()

    val dfJoin1 = dsFornitura.as(forn).join(dsPod.as(pods),
      dsFornitura(RcuFornituraPSchema.n_id_pod)=== dsPod(RcuPodPSchema.n_id_pod),
      "inner")
      .join(dfPodStatoAggregated.as(stato_pods),
        dsPod(RcuFornituraPSchema.n_id_pod)=== dfPodStatoAggregated(RcuPodStatoPSchema.n_id_pod),
        "inner")
      .withColumn(n_id_fornitura_numeric,sqlToSparkUtilitties.isNumericUDF(col(RcuFornituraPSchema.n_id_fornitura)))
      .filter(col(n_id_fornitura_numeric) === true)
      .select(
              dsFornitura(RcuFornituraPSchema.n_id_fornitura),
              coalesce(dsFornitura(RcuFornituraPSchema.d_inizio_titolarita), lit("")).alias(d_inizio),
              coalesce(dsFornitura(RcuFornituraPSchema.d_fine_titolarita), lit("")).alias(d_fine),
              substring(dsPod(RcuPodPSchema.t_codice_pod), 1, 14).alias(codice_pod),
              when(coalesce(dfPodStatoAggregated(RcuPodStatoPSchema.t_stato_attivazione), lit("")) === "N", lit("0"))
                .otherwise(lit("1"))
                .alias(attivo),
              dsFornitura(RcuFornituraPSchema.n_id_pod),
              coalesce(dsFornitura(RcuFornituraPSchema.n_id_fornitore), lit("")).alias(n_id_fornitore),
              dsFornitura(RcuFornituraPSchema.n_id_cliente),
              dsFornitura(RcuFornituraPSchema.t_tipo_mercato),
              dsPod(RcuPodPSchema.n_id_indirizzo),
              dsPod(RcuPodPSchema.n_id_ind_forn),
              coalesce(dsFornitura(RcuFornituraPSchema.t_servizio_tutela_sii), lit("")).alias(t_servizio_tutela_sii)
    )  .persist()

    dfPodStatoAggregated.unpersist()


//    SECOND PART TBL_DATA
    // Filter rows based on the condition T_STATO_ATTIVAZIONE = 'A' OR NVL(T_STATO_ATTIVAZIONE, '') = ''
    def  dsRcusPodstatoPModelPrepared = dsRcusPodStato
      .filter(col(RcusPodstatoPSchema.b_valido).equalTo("Y")).persist()

    val dfRcusPodStatoAggregated = dsRcusPodstatoPModelPrepared
      .groupBy(col(RcusPodstatoPSchema.n_id_pod),col(RcusPodstatoPSchema.t_stato_attivazione))
      .agg(max(RcusPodstatoPSchema.d_aggiornamento).alias(MAX_DATA_AGGIORNAMENTO))
      .select(col(RcusPodstatoPSchema.n_id_pod),
        col(MAX_DATA_AGGIORNAMENTO),
        col(RcusPodstatoPSchema.t_stato_attivazione)
      ).persist()

    val dfJoin2 = dsRcusFornitura.as(forn).join(dsPod.as(pods),
      dsRcusFornitura(RcusFornituraPSchema.n_id_pod)=== dsPod(RcuPodPSchema.n_id_pod),
      "inner")
      .join(dfRcusPodStatoAggregated.as(stato_pods),
        dsPod(RcusPodstatoPSchema.n_id_pod)=== dfRcusPodStatoAggregated(RcuPodPSchema.n_id_pod)
        ,"inner")
      .withColumn(n_id_fornitura_numeric,sqlToSparkUtilitties.isNumericUDF(col(RcusFornituraPSchema.n_id_fornitura)))
      .filter(
        concat(coalesce(dsRcusFornitura(RcusFornituraPSchema.d_inizio_titolarita), lit("")),
          coalesce(dsRcusFornitura(RcuFornituraPSchema.d_fine_titolarita), lit(""))) =!= ""
          && col(n_id_fornitura_numeric) === true
          //b_valido belong to dsRcusFornitura, not to dfRcusPodStatoAggregated
          && dsRcusFornitura(RcusFornituraPSchema.b_valido) === "N"
      )
      .select(
          dsRcusFornitura(RcusFornituraPSchema.n_id_fornitura),
          coalesce(dsRcusFornitura(RcusFornituraPSchema.d_inizio_titolarita), lit("")).alias(d_inizio),
          coalesce(dsRcusFornitura(RcusFornituraPSchema.d_fine_titolarita), lit("")).alias(d_fine),
          substring(dsPod(RcuPodPSchema.t_codice_pod), 1, 14).alias(codice_pod),
          lit("0").alias(attivo),
          dsRcusFornitura(RcusFornituraPSchema.n_id_pod),
          coalesce(dsRcusFornitura(RcusFornituraPSchema.n_id_fornitore), lit("")).alias(n_id_fornitore),
          dsRcusFornitura(RcusFornituraPSchema.n_id_cliente),
          dsRcusFornitura(RcusFornituraPSchema.t_tipo_mercato),
          dsPod(RcuPodPSchema.n_id_indirizzo),
          dsPod(RcuPodPSchema.n_id_ind_forn),
          lit("").alias(t_servizio_tutela_sii)
      ).persist()

    dfRcusPodStatoAggregated.unpersist()

    //define TBL_DATA

    // Transformations for 'inizio' and 'fine' fields
    def inizioExpr = when(col(d_inizio) === "",
      concat(year(costants.dateMinus1126), lpad(month(costants.dateMinus1126), 2, "0"), lit("01"))
    ).otherwise(
      concat(substring(col(d_inizio), 1, 4), substring(col(d_inizio), 6, 2), substring(col(d_inizio), 9, 2))
    )

    def fineExpr = when(col(d_fine) === "",
      concat(year(costants.currentDate), lpad(month(costants.currentDate), 2, "0"), lpad(dayofmonth(costants.currentDate), 2, "0"))
    ).otherwise(
      concat(substring(col(d_fine), 1, 4), substring(col(d_fine), 6, 2), substring(col(d_fine), 9, 2))
    )

    // Expressions for 'd_inizio_str' and 'd_fine_str'
    def dInizioStrExpr = when(col(d_inizio) === "", lit(""))
      .otherwise(concat(substring(col(d_inizio), 1, 4), substring(col(d_inizio), 6, 2), substring(col(d_inizio), 9, 2)))

    def dFineStrExpr = when(col(d_fine) === "", lit(""))
      .otherwise(concat(substring(col(d_fine), 1, 4), substring(col(d_fine), 6, 2), substring(col(d_fine), 9, 2)))

    // Expression for 'd_inizio_start'
    def dInizioStartExpr = concat(year(costants.dateMinus1126), lpad(month(costants.dateMinus1126), 2, "0"), lit("01"))

    val tbl_data = dfJoin1.unionByName(dfJoin2)
      .select(
              col(n_id_fornitura),
              inizioExpr.cast(LongType).alias(inizio),
              fineExpr.cast(LongType).alias(fine),
              dInizioStrExpr.alias(d_inizio_str),
              dFineStrExpr.alias(d_fine_str),
              col(codice_pod),
              col(attivo),
              col(n_id_pod),
              col(n_id_fornitore),
              col(n_id_cliente),
              col(t_tipo_mercato),
              col(n_id_indirizzo),
              col(n_id_ind_forn),
              dInizioStartExpr.cast(LongType).alias(d_inizio_start),
              col(t_servizio_tutela_sii)
      ).persist()

    //define tbl

    dfJoin1.unpersist()
    dfJoin2.unpersist()

    // Define a window specification for the MIN function
    def cod_pod_fineWindow = Window.partitionBy(codice_pod, fine)

    // Apply transformations
    val tbl = tbl_data
      .withColumn(n_id_indirizzo, coalesce(col(n_id_indirizzo), lit("")))
      .withColumn(n_id_ind_forn, coalesce(col(n_id_ind_forn), lit("")))
      .withColumn(min_inizio, min(col(inizio)).over(cod_pod_fineWindow))
      .filter(
        col(fine) >= col(d_inizio_start) &&
          (col(inizio) + 2) < col(fine)
      )
      .select(
        col(n_id_fornitura),
        col(inizio),
        col(fine),
        col(codice_pod),
        col(attivo),
        col(n_id_pod),
        col(n_id_fornitore),
        col(n_id_cliente),
        col(t_tipo_mercato),
        col(n_id_indirizzo),
        col(n_id_ind_forn),
        col(min_inizio),
        col(d_inizio_str),
        col(d_fine_str),
        col(d_inizio_start),
        col(t_servizio_tutela_sii)
      )
      .persist()
    //definition ttx

    tbl_data.unpersist()

    // Transformations
    val ttx = tbl
              .withColumn(n_id_indirizzo, coalesce(col(n_id_indirizzo), lit("")))
              .withColumn(n_id_ind_forn, coalesce(col(n_id_ind_forn), lit("")))
              .withColumn(min_inizio, min(col(inizio)).over(cod_pod_fineWindow))
              .filter(
                //todo check properly
                // d_inizio_start is equal to CAST(CONCAT(YEAR(DATE_SUB(CURRENT_DATE, 1126)), LPAD(MONTH(DATE_SUB(CURRENT_DATE, 1126)), 2, 0), '01') AS BIGINT)
                col(fine) >= col(d_inizio_start) &&
                  (col(inizio) + 2) < col(fine)
              )
              .select(
                col(n_id_fornitura),
                col(inizio),
                col(fine),
                col(codice_pod),
                col(attivo),
                col(n_id_pod),
                col(n_id_fornitore),
                col(n_id_cliente),
                col(t_tipo_mercato),
                col(n_id_indirizzo),
                col(n_id_ind_forn),
                col(min_inizio),
                col(d_inizio_str),
                col(d_fine_str),
                col(d_inizio_start),
                col(t_servizio_tutela_sii)
              )
      .distinct()
      .persist()

    //forniture_tmp
      val forniture_tmp = ttx
        .select(
          col(n_id_fornitura),
          col(inizio),
          col(d_inizio_start),
          col(fine),
          col(d_inizio_str),
          col(d_fine_str),
          col(codice_pod),
          col(attivo),
          col(n_id_pod),
          col(n_id_fornitore),
          col(t_tipo_mercato),
          col(n_id_cliente),
          col(n_id_indirizzo),
          col(n_id_ind_forn),
          col(t_servizio_tutela_sii)
        )
      .filter(col(inizio) === col(min_inizio)) // Apply the WHERE condition
        .withColumn(inizio, when(col(inizio) < col(d_inizio_start), col(d_inizio_start)).otherwise(col(inizio))) // Apply the CASE logic
        .drop(col(d_inizio_start))
        //.dropDuplicates(n_id_fornitura) // Ensure DISTINCT rows
        .distinct()
        .persist()

    ttx.unpersist()

    // Aggregate to compute k_key
    val aggregated_tmp = forniture_tmp
      .groupBy(n_id_fornitura, inizio)
      .agg(
        max(fine).alias(max_fine)
      )
      .withColumn(k_key, concat(col(n_id_fornitura), col(max_fine)))

    // Add k_key to the original DataFrame
    val forniture_tmp_withKey = forniture_tmp
      .withColumn(k_key, concat(col(n_id_fornitura), col(fine)))

    forniture_tmp.unpersist()

    // Perform the inner join
    val forniture = forniture_tmp_withKey
      .join(aggregated_tmp.select(k_key), Seq(k_key), "inner")
      .drop(k_key) // Drop the key column if not needed

    // Forniture Dataset
      val fornitureDataset : Dataset[FornitureModel] = forniture.selectExpr( FornitureSchema.getValues: _* )
      .as[FornitureModel]

    fornitureDataset

  }

  def calcolo_forniture_info(dsFornitura: Dataset[FornitureModel],
                             dsResidenza :Dataset[RcuResidenzaPModel],
                             dsTariffa:Dataset[RcuTariffaPModel],
                             dsAzienda :  Dataset[RcuAziendaPModel]):Dataset[FornitureInfoModel] = {

    val dsResidenzaFiltered = dsResidenza.filter(col(RcuResidenzaPSchema.b_valido).equalTo("Y")
    && upper(col(RcuResidenzaPSchema.b_ultima)).equalTo("Y")
    &&  upper(col(RcuResidenzaPSchema.b_storico)).equalTo("O")
    ).persist()

    val dsTariffaFiltered = dsTariffa.filter(col(RcuTariffaPSchema.b_valido).equalTo("Y")
      && upper(col(RcuTariffaPSchema.b_ultima)).equalTo("Y")
      &&  upper(col(RcuTariffaPSchema.b_storico)).equalTo("O")
    ).persist()

    val dsAziendaFiltered = dsAzienda
      .withColumn(n_id_azienda_numeric,sqlToSparkUtilitties.isNumericUDF(col(RcuAziendaPSchema.n_id_azienda)))
      .filter(col(n_id_azienda_numeric) === true).persist()

    val dsJoin = dsFornitura.as(forniture).join(
      dsResidenzaFiltered.as(residenza)
      , dsFornitura(FornitureSchema.n_id_fornitura) === dsResidenzaFiltered(RcuResidenzaPSchema.n_id_fornitura)
      , "left"
    )
      .join(dsTariffaFiltered.as(tariffa)
        , dsFornitura(FornitureSchema.n_id_fornitura) === dsTariffaFiltered(RcuTariffaPSchema.n_id_fornitura)
        , "left"
      )
      .join(broadcast(dsAziendaFiltered).as(azienda)
        , dsFornitura(FornitureSchema.n_id_fornitore) === dsAziendaFiltered(RcuAziendaPSchema.n_id_azienda)
        , "left"
      )
      .select(
        dsFornitura(FornitureSchema.n_id_fornitura),
        dsFornitura(FornitureSchema.n_id_pod),
        dsFornitura(FornitureSchema.n_id_cliente),
        dsFornitura(FornitureSchema.inizio).as(FornitureInfoSchema.d_inizio_titolarita),
        dsFornitura(FornitureSchema.fine).as(FornitureInfoSchema.d_fine_titolarita),
        dsFornitura(FornitureSchema.d_inizio_str).as(FornitureInfoSchema.d_inizio_titolarita_str),
        dsFornitura(FornitureSchema.d_fine_str).as(FornitureInfoSchema.d_fine_titolarita_str),
        dsFornitura(FornitureSchema.n_id_fornitore),
        dsFornitura(FornitureSchema.t_tipo_mercato),
        dsFornitura(FornitureSchema.n_id_indirizzo),
        dsFornitura(FornitureSchema.n_id_ind_forn),
        dsFornitura(FornitureSchema.codice_pod),
        dsResidenzaFiltered(RcuResidenzaPSchema.t_residente),
        dsTariffaFiltered(RcuTariffaPSchema.t_tariffa_distr),
        dsAziendaFiltered(RcuAziendaPSchema.t_piva),
        dsAziendaFiltered(RcuAziendaPSchema.t_rag_soc),
        dsFornitura(FornitureSchema.t_servizio_tutela_sii)
      ).selectExpr(FornitureInfoSchema.getValues:_*)
     .as[FornitureInfoModel]

    dsJoin
  }

}
