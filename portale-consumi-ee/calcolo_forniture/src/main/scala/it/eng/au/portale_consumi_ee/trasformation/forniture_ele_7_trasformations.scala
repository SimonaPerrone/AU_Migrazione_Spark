package it.eng.au.portale_consumi_ee.trasformation

import it.eng.au.portale_consumi_ee.common.model.mongodbs.fornitureElettricheModel
import it.eng.au.portale_consumi_ee.common.schema.mongodbs.fornitureElettricheSchema
import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.common.utility.functions.costants.DOT
import it.eng.au.portale_consumi_ee.common.utility.functions.{costants, sqlToSparkUtilitties}
import it.eng.au.portale_consumi_ee.model.mongodbs.{FornitureInfoModel, FornitureModel, GdmModel, RcuPodDistrModel, SwitchModel, fasceModel, podModel}
import it.eng.au.portale_consumi_ee.model.rcu.{RcuAziendaPModel, RcuClienteFinalePModel, RcuIndirizzoPModel, RcuPodPModel, rcuCodiceOffertaPModel}
import it.eng.au.portale_consumi_ee.model.tde.tdeVulnPModel
import it.eng.au.portale_consumi_ee.schema.mongodbs.{FornitureInfoSchema, FornitureSchema, GdmSchema, RcuPodDistrSchema, SwitchSchema, fasceSchema, podSchema, rcuCodiceOffertaPSchema}
import it.eng.au.portale_consumi_ee.schema.rcu.{RcuAziendaPSchema, RcuClienteFinalePSchema, RcuIndirizzoPSchema, RcuPodPSchema}
import it.eng.au.portale_consumi_ee.schema.tde.tdeVulnPSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, StringType}
import org.apache.spark.sql.{Column, Dataset, SparkSession}

// spark implementation of hql_forniture_ele_7_human_readble.sql
object forniture_ele_7_trasformations {

  val spark: SparkSession = Environment.getSpark

  import spark.implicits._

  //costanti calcolo pod
  val n_id_azienda_number = "n_id_azienda_number"

  val  forniture_info = "forniture_info"
  val switch = "switch"
  val n_id_fornitura_switch = "n_id_fornitura_switch"
  val n_id_fornitura_gdm = "n_id_fornitura_gdm"
  val n_id_fornitura_gdm_cambio = "n_id_fornitura_gdm_cambio"
  val rcu_indirizzo = "rcu_indirizzo"
  val gdm = "gdm"
  val gdm_cambio = "gdm_cambio"
  val rcu_azienda = "rcu_azienda"

  //costanti calcolo forniture_elettriche
  val t_cf_piva = "t_cf_piva"
  val fasce_in = "fasce_in"
  val forn = "forn"
  val rcu_clientefinale = "rcu_clientefinale"
  val pod = "pod"
  val rcu_pod_distr= "rcu_pod_distr"
  val fasce = "fasce"
  val t_cf = "t_cf"

  val v1 = "v1"
  val tabPod = "tabPod"
  val tabB = "tabB"
  val p = "p"
  val AA = "AA"
  val tabF = "tabF"
  val max_d_aggiornamento = "max_d_aggiornamento"
  val B = "B"
  val A = "A"
  val t_codice_pod_old = "t_codice_pod_old"

  // see hql_forniture_ele_7_human_readble.sql
  def calcolo_pod(
                   dsFornitureInfo : Dataset[FornitureInfoModel],
                   dsSwitch: Dataset[SwitchModel],
                   dsGdm: Dataset[GdmModel],
                   dsForniture: Dataset[FornitureModel],
                   dsRcuIndirizzoP: Dataset[RcuIndirizzoPModel],
                   dsRcuAziendaP:Dataset[RcuAziendaPModel]
                 ): Dataset[podModel] = {

    val dfSwitch = dsSwitch.join(
      dsFornitureInfo,
      dsSwitch(SwitchSchema.t_codice_pod) === dsFornitureInfo(FornitureInfoSchema.codice_pod) &&
        dsSwitch(SwitchSchema.n_id_cliente) === dsFornitureInfo(FornitureInfoSchema.n_id_cliente),
        "inner"
      )
      .filter(
        dsSwitch(SwitchSchema.data_switch) >= dsFornitureInfo(FornitureInfoSchema.d_inizio_titolarita) &&
          dsSwitch(SwitchSchema.data_switch) <= when(
            dsFornitureInfo(FornitureInfoSchema.d_fine_titolarita_str).equalTo(""),
            dsSwitch(SwitchSchema.data_switch)
          ).otherwise(dsFornitureInfo(FornitureInfoSchema.d_fine_titolarita))
      )
      .select(
        dsSwitch("*"),
        dsFornitureInfo(FornitureInfoSchema.n_id_fornitura).as(n_id_fornitura_switch)
      )
      .persist()

    val dfGdm = dsGdm
      .join(
        dsForniture,
        dsGdm(GdmSchema.n_id_pod) === dsForniture("n_id_pod"),
        "inner"
      )
      .filter(
        coalesce(dsGdm(GdmSchema.d_inst_misurator_att), lit(0)) === 0 ||
          dsGdm(GdmSchema.d_inst_misurator_att) <= dsForniture(FornitureSchema.fine)
      )
      .select(
        dsGdm("*"),
        dsForniture(FornitureSchema.n_id_fornitura).as(n_id_fornitura_gdm)
      )
      .persist()

    val dfgdmCambio = dsGdm
    .join(dsForniture, dsGdm(FornitureSchema.n_id_pod) === dsForniture(FornitureSchema.n_id_pod))
      .filter(
        (dsGdm(GdmSchema.data_cambio_gdm) >= dsForniture(FornitureSchema.inizio) && dsGdm(GdmSchema.data_cambio_gdm) <= dsForniture(FornitureSchema.fine)) ||
          (dsGdm(GdmSchema.d_inst_misurator_att) >= dsForniture(FornitureSchema.inizio) && dsGdm(GdmSchema.d_inst_misurator_att) <= dsForniture(FornitureSchema.fine))
      )
      .select(dsGdm("*"), dsForniture(FornitureSchema.n_id_fornitura).as(n_id_fornitura_gdm_cambio))
      .persist()

    val dfRcuAzienda = dsRcuAziendaP
    .withColumn(n_id_azienda_number,sqlToSparkUtilitties.isNumericUDF(col(RcuAziendaPSchema.n_id_azienda)))
      .filter(col(n_id_azienda_number) === true)
      .select(
        col(RcuAziendaPSchema.n_id_azienda),
        col(RcuAziendaPSchema.t_rag_soc),
        col(RcuAziendaPSchema.t_piva)
      )

    val preFinalDF  = dsFornitureInfo.as(forniture_info)
      .join(dfSwitch.as(switch),
      dfSwitch(SwitchSchema.t_codice_pod)===dsFornitureInfo(FornitureInfoSchema.codice_pod) &&
        dfSwitch(n_id_fornitura_switch)===dsFornitureInfo(FornitureInfoSchema.n_id_fornitura) ,"left")
      .join(dsRcuIndirizzoP.as(rcu_indirizzo),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.n_id)===coalesce(dsFornitureInfo(FornitureInfoSchema.n_id_indirizzo),dsFornitureInfo(FornitureInfoSchema.n_id_ind_forn))
        ,"left")
      .join(dfGdm.as(gdm)
        , dfGdm(GdmSchema.n_id_pod)===dsFornitureInfo(FornitureInfoSchema.n_id_pod) && dfGdm(n_id_fornitura_gdm)===dsFornitureInfo(FornitureInfoSchema.n_id_fornitura)
        ,"left")
      .join(dfgdmCambio.as(gdm_cambio)
        ,dfgdmCambio(GdmSchema.n_id_pod) === dsFornitureInfo(FornitureInfoSchema.n_id_pod) && dfgdmCambio(n_id_fornitura_gdm_cambio) === dsFornitureInfo(FornitureInfoSchema.n_id_fornitura)
        ,"left")
      .join(broadcast(dfRcuAzienda).as(rcu_azienda)
        , coalesce(dsFornitureInfo(FornitureInfoSchema.n_id_fornitore),lit(""))===dfRcuAzienda(RcuAziendaPSchema.n_id_azienda)
        ,"left")
      .select(
        dsFornitureInfo(FornitureInfoSchema.n_id_pod),
        dsFornitureInfo(FornitureInfoSchema.codice_pod).alias("t_codice_pod"),
        when(dsFornitureInfo(FornitureInfoSchema.n_id_indirizzo).isNull, dsFornitureInfo(FornitureInfoSchema.n_id_ind_forn))
          .otherwise(dsFornitureInfo(FornitureInfoSchema.n_id_indirizzo))
          .alias(podSchema.id_indirizzo),
        dsFornitureInfo(FornitureInfoSchema.n_id_fornitura),
        dsFornitureInfo(FornitureInfoSchema.n_id_cliente),
        dsFornitureInfo(FornitureInfoSchema.d_inizio_titolarita),
        dsFornitureInfo(FornitureInfoSchema.d_inizio_titolarita_str),
        dsFornitureInfo(FornitureInfoSchema.d_fine_titolarita),
        dsFornitureInfo(FornitureInfoSchema.d_fine_titolarita_str),
        when(dsFornitureInfo(FornitureInfoSchema.t_piva).isin("00000000012", "00000000010", "00000000011"), lit("S"))
          .otherwise(dsFornitureInfo(FornitureInfoSchema.t_tipo_mercato))
          .alias(podSchema.tipo_mercato),
        dsFornitureInfo(FornitureInfoSchema.n_id_fornitore),
        dsFornitureInfo(FornitureInfoSchema.t_residente),
        dsFornitureInfo(FornitureInfoSchema.t_tariffa_distr).alias(podSchema.tariffa),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.t_toponimo),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.t_nomestrada),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.t_civico),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.t_comune),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.t_cap),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.t_provincia),
        dsRcuIndirizzoP(RcuIndirizzoPSchema.t_nazione),
        dfGdm(GdmSchema.n_potenza_disponibile).alias(podSchema.potenza_disponibile),
        dfGdm(GdmSchema.n_potenza_impegnata).alias(podSchema.potenza_impegnata),
        dfGdm(GdmSchema.n_tensione).alias(podSchema.tensione),
        dfGdm(GdmSchema.t_tipo_misuratore).alias(podSchema.tipo_misuratore),
        col(gdm_cambio + "." + GdmSchema.cambio_gdm),
        col(gdm_cambio + "." + GdmSchema.data_cambio_gdm),
        col(gdm_cambio + "." + GdmSchema.data_cambio_gdm_str),
        col(gdm_cambio + "." + GdmSchema.d_inst_misurator_att),
        dfGdm(GdmSchema.stato_misuratore_2g),
        dfGdm(GdmSchema.trattamento),
        dfSwitch(SwitchSchema.data_switch),
        dfSwitch(SwitchSchema.switching_in_corso),
        dfGdm(GdmSchema.d_oper_misurator_att),
        dfGdm(GdmSchema.d_oper_misurator_att_str),
        dfGdm(GdmSchema.t_mat_misuratore_att).alias(podSchema.matricola_misuratore),
        dfGdm(GdmSchema.anno_start_misure_orarie),
        dfGdm(GdmSchema.mese_start_misure_orarie),
        dsFornitureInfo(FornitureInfoSchema.t_piva),
        dsFornitureInfo(FornitureInfoSchema.t_rag_soc),
        dsFornitureInfo(FornitureInfoSchema.t_servizio_tutela_sii)
      ).distinct()
      .selectExpr(podSchema.getValues:_*)
      .as[podModel]

    dfSwitch.unpersist()
    dfGdm.unpersist()
    dfgdmCambio.unpersist()

    preFinalDF
  }


  def getStatoPod(d_oper_misurator_att: Column): Column = {
    val currentDate = current_date()
    val currentYearMonthDay = date_format(currentDate, "yyyyMMdd").cast(LongType)

    val dOperString = d_oper_misurator_att.cast(StringType)
    val dOperDate = concat_ws("-",
      substring(dOperString, 1, 4),
      substring(dOperString, 5, 2),
      substring(dOperString, 7, 2)
    ).cast(DateType)

    when(d_oper_misurator_att > currentYearMonthDay, "IN FUNZIONE")
      .when(
        dayofmonth(dOperDate) < 16 && currentDate > dOperDate,
        when(
          currentDate < add_months(
            concat_ws("-",
              substring(dOperString, 1, 4),
              substring(dOperString, 5, 2),
              lit("01")
            ).cast(DateType), 2
          ),
          "ATTIVO"
        ).otherwise("COMPLETAMENTE CONFIGURABILE")
      )
      .when(
        dayofmonth(dOperDate) > 15 && currentDate > dOperDate,
        when(
          currentDate < add_months(
            concat_ws("-",
              substring(dOperString, 1, 4),
              substring(dOperString, 5, 2),
              lit("01")
            ).cast(DateType), 3
          ),
          "ATTIVO"
        ).otherwise("COMPLETAMENTE CONFIGURABILE")
      )
      .otherwise("")
  }


  def calcolo_forniture_elettriche(
                                  dsRcuClienteFinaleP : Dataset[RcuClienteFinalePModel],
                                  dsPod: Dataset[podModel],
                                  dsRcuPodDistr : Dataset[RcuPodDistrModel],
                                  dsfasce: Dataset[fasceModel],
                                  dsForniture: Dataset[FornitureModel],
                                  dsTdeVulnP: Dataset[tdeVulnPModel],
                                  dsRcuPodP: Dataset[RcuPodPModel],
                                  dsRcuCodiceOffertaP : Dataset[rcuCodiceOffertaPModel]
                 ): Dataset[fornitureElettricheModel] = {

    // definition forniture_elettriche1

    val dfRcu_clientefinale = dsRcuClienteFinaleP
    .withColumn(
      t_cf_piva,
      when(col(RcuClienteFinalePSchema.t_cf).isNull || col(RcuClienteFinalePSchema.t_cf) === "", col(RcuClienteFinalePSchema.t_piva))
        .otherwise(col(RcuClienteFinalePSchema.t_cf))
    )
      .filter(
        concat_ws("", coalesce(col(RcuClienteFinalePSchema.t_cf), lit("")), coalesce(col(RcuClienteFinalePSchema.t_piva), lit(""))) =!= ""
      )
      .persist()

    val dfFasce = dsfasce.as(fasce_in).join(dsForniture.as(forn),dsfasce(fasceSchema.n_id_pod) === dsForniture(FornitureSchema.n_id_pod)
      , "left_outer")
      .filter(
        dsForniture(FornitureSchema.n_id_fornitura).isNotNull &&
          dsfasce(fasceSchema.d_fine_validita) >= dsForniture(FornitureSchema.inizio) &&
          dsfasce(fasceSchema.d_fine_validita) <= dsForniture(FornitureSchema.fine)
      )
      .select(dsForniture(FornitureSchema.n_id_fornitura),
        dsfasce("*"))
      .persist()

    val forniture_elettriche_1Join = dfRcu_clientefinale.as(rcu_clientefinale).join(dsPod.as(pod),
      dfRcu_clientefinale(RcuClienteFinalePSchema.n_id_cliente) === dsPod(podSchema.n_id_cliente)
      ,"inner")
      .join(dsRcuPodDistr.as(rcu_pod_distr),
        dsPod(podSchema.n_id_pod)===dsRcuPodDistr(RcuPodDistrSchema.n_id_pod),
        "left")
      .join(dfFasce.as(fasce), dsPod(podSchema.n_id_pod)===dfFasce(fasceSchema.n_id_pod) && dsPod(podSchema.n_id_fornitura)===dfFasce(FornitureSchema.n_id_fornitura),"left_outer")
      .select(
        coalesce(dsPod(podSchema.n_id_pod), lit("")).alias(podSchema.n_id_pod),
        coalesce(dsPod(podSchema.n_id_cliente), lit("")).alias(podSchema.n_id_cliente),
        coalesce(dfRcu_clientefinale(t_cf_piva), lit("")).alias(t_cf),
        coalesce(dfRcu_clientefinale(RcuClienteFinalePSchema.t_nome), lit("")).alias(RcuClienteFinalePSchema.t_nome),
        coalesce(dfRcu_clientefinale(RcuClienteFinalePSchema.t_cognome), lit("")).alias(RcuClienteFinalePSchema.t_cognome),
        coalesce(dfRcu_clientefinale(RcuClienteFinalePSchema.t_piva), lit("")).alias(RcuClienteFinalePSchema.t_piva),
        coalesce(dfRcu_clientefinale(RcuClienteFinalePSchema.t_ragsoc), lit("")).alias(RcuClienteFinalePSchema.t_ragsoc),
        coalesce(dsPod(podSchema.t_codice_pod), lit("")).alias(fornitureElettricheSchema.codice_pod),
        coalesce(dsPod(podSchema.n_id_fornitura), lit("")).alias(fornitureElettricheSchema.codice_fornitura),
        coalesce(dsPod(podSchema.d_inizio_titolarita), lit(0)).alias(fornitureElettricheSchema.data_inizio_fornitura_num),
        coalesce(dsPod(podSchema.d_inizio_titolarita_str), lit("")).alias(fornitureElettricheSchema.data_inizio_fornitura),
        coalesce(dsPod(podSchema.d_fine_titolarita_str), lit("")).alias(fornitureElettricheSchema.data_fine_fornitura),
        coalesce(dsPod(podSchema.d_fine_titolarita), lit(0)).alias(fornitureElettricheSchema.data_fine_fornitura_num),
        coalesce(dsPod(podSchema.tipo_mercato), lit("")).alias(fornitureElettricheSchema.tipo_mercato),
        coalesce(dsPod(podSchema.t_residente), lit("")).alias(fornitureElettricheSchema.residente),
        coalesce(dsPod(podSchema.tariffa), lit("")).alias(fornitureElettricheSchema.tariffa),
        coalesce(dsPod(podSchema.tensione), lit("")).alias(fornitureElettricheSchema.tensione),
        coalesce(dsPod(podSchema.potenza_disponibile), lit("")).alias(fornitureElettricheSchema.potenza_disponibile),
        coalesce(dsPod(podSchema.potenza_impegnata), lit("")).alias(fornitureElettricheSchema.potenza_impegnata),
        coalesce(dsPod(podSchema.tipo_misuratore), lit("")).alias(fornitureElettricheSchema.tipo_misuratore),
        when(dsPod(podSchema.tipo_misuratore) === "G", getStatoPod(col(podSchema.d_oper_misurator_att)))
          .otherwise("").alias(fornitureElettricheSchema.stato_misuratore_2g),
        coalesce(dsPod(podSchema.t_toponimo), lit("")).alias(fornitureElettricheSchema.toponimo),
        coalesce(dsPod(podSchema.t_nomestrada), lit("")).alias(fornitureElettricheSchema.nome_strada),
        coalesce(dsPod(podSchema.t_civico), lit("")).alias(fornitureElettricheSchema.civico),
        coalesce(dsPod(podSchema.t_comune), lit("")).alias(fornitureElettricheSchema.comune),
        coalesce(dsPod(podSchema.t_cap), lit("")).alias(fornitureElettricheSchema.cap),
        coalesce(dsPod(podSchema.t_provincia), lit("")).alias(fornitureElettricheSchema.provincia),
        coalesce(dsPod(podSchema.t_nazione), lit("")).alias(fornitureElettricheSchema.nazione),
        coalesce(dsPod(podSchema.trattamento), lit("")).alias(fornitureElettricheSchema.trattamento),
        lit("").alias(fornitureElettricheSchema.data_inizio_processo_gdm),
        lit("").alias(fornitureElettricheSchema.data_fine_processo_gdm),
        coalesce(dsPod(podSchema.d_inst_misurator_att), lit("")).alias(fornitureElettricheSchema.data_inizio_validita_gdm),
        when(dsPod(podSchema.d_inst_misurator_att).isNull || dsPod(podSchema.d_inst_misurator_att) === "", "")
          .otherwise("PRO001").alias(fornitureElettricheSchema.id_processo_gdm),
        //todo verify if it is correct
        when(dsPod(podSchema.data_cambio_gdm) >= date_format(costants.currentDate, "yyyyMMdd").cast(LongType), "true")
          .otherwise("false").alias(fornitureElettricheSchema.in_corso_gdm),
        when(dsPod(podSchema.d_inst_misurator_att).isNull || dsPod(podSchema.d_inst_misurator_att) === "", "")
          .otherwise("note").alias(fornitureElettricheSchema.note_gdm),
        when(dsPod(podSchema.d_inst_misurator_att).isNull || dsPod(podSchema.d_inst_misurator_att) === "", "")
          .otherwise("cambio_gdm").alias(fornitureElettricheSchema.tipo_processo_gdm),
        lit("").alias(fornitureElettricheSchema.data_inizio_processo_switch),
        lit("").alias(fornitureElettricheSchema.data_fine_processo_switch),
        when(dsPod(podSchema.data_switch).isNull || dsPod(podSchema.data_switch) === 19700101, "")
          .otherwise(col("data_switch").cast("string")).alias(fornitureElettricheSchema.data_inizio_validita_switch),
        when(dsPod(podSchema.data_switch).isNull || dsPod(podSchema.data_switch) === 19700101, "")
          .otherwise("PRO002").alias(fornitureElettricheSchema.id_processo_switch),
        coalesce(dsPod(podSchema.switching_in_corso), lit("")).alias(fornitureElettricheSchema.in_corso_switch),
        when(dsPod(podSchema.data_switch).isNull || dsPod(podSchema.data_switch) === 19700101, "")
          .otherwise("note").alias(fornitureElettricheSchema.note_switch),
        when(dsPod(podSchema.data_switch).isNull || dsPod(podSchema.data_switch) === 19700101, "")
          .otherwise("switch").alias(fornitureElettricheSchema.tipo_processo_switch),
        coalesce(dsPod(podSchema.matricola_misuratore), lit("")).alias(fornitureElettricheSchema.matricola_misuratore),
        coalesce(dsPod(podSchema.t_piva), lit("")).alias(fornitureElettricheSchema.p_iva_cc),
        coalesce(dsPod(podSchema.t_rag_soc), lit("")).alias(fornitureElettricheSchema.ragione_sociale_cc),
        coalesce(dsRcuPodDistr(RcuPodDistrSchema.t_rag_soc), lit("")).alias(fornitureElettricheSchema.ragione_sociale_distributore),
        coalesce(dfFasce(fasceSchema.f_lunedi), lit("")).alias(fornitureElettricheSchema.f_lunedi),
        coalesce(dfFasce(fasceSchema.f_martedi), lit("")).alias(fornitureElettricheSchema.f_martedi),
        coalesce(dfFasce(fasceSchema.f_mercoledi), lit("")).alias(fornitureElettricheSchema.f_mercoledi),
        coalesce(dfFasce(fasceSchema.f_giovedi), lit("")).alias(fornitureElettricheSchema.f_giovedi),
        coalesce(dfFasce(fasceSchema.f_venerdi), lit("")).alias(fornitureElettricheSchema.f_venerdi),
        coalesce(dfFasce(fasceSchema.f_sabato), lit("")).alias(fornitureElettricheSchema.f_sabato),
        coalesce(dfFasce(fasceSchema.f_domenica), lit("")).alias(fornitureElettricheSchema.f_domenica),
        coalesce(dfFasce(fasceSchema.f_festivo), lit("")).alias(fornitureElettricheSchema.f_festivo),
        coalesce(dfFasce(fasceSchema.d_inizio_validita), lit("")).alias(fornitureElettricheSchema.d_inizio_validita_fascia),
        coalesce(dfFasce(fasceSchema.d_fine_validita_str), lit("")).alias(fornitureElettricheSchema.d_fine_validita_fascia),
        coalesce(dfFasce(fasceSchema.d_data_iniziofreezing), lit("")).alias(fornitureElettricheSchema.d_data_iniziofreezing),
        coalesce(dfFasce(fasceSchema.n_id_misuratore), lit("")).alias(fornitureElettricheSchema.id_misuratore_fasce),
        coalesce(dsPod(podSchema.t_servizio_tutela_sii), lit("")).alias(fornitureElettricheSchema.t_servizio_tutela_sii)
      ).orderBy(fornitureElettricheSchema.codice_pod)
      .persist()

    dfRcu_clientefinale.unpersist()
    dfFasce.unpersist()

    //calcolo forniture_elettriche2

    val dfTabPod = dsTdeVulnP
      .join(dsRcuPodP, dsTdeVulnP(tdeVulnPSchema.n_id_pod) === dsRcuPodP(RcuPodPSchema.n_id_pod), "inner")
      .select(dsRcuPodP(RcuPodPSchema.t_codice_pod), dsTdeVulnP("*"))
      .persist()

    val dfTabB = dsTdeVulnP
      .alias(v1)
      .join(
        dfTabPod.alias(tabPod),
        dsTdeVulnP(tdeVulnPSchema.n_id_cliente) === dfTabPod(tdeVulnPSchema.n_id_cliente),
        "inner"
      )
      .withColumnRenamed(RcuPodPSchema.t_codice_pod,t_codice_pod_old)
      .withColumn(RcuPodPSchema.t_codice_pod,substring(col(t_codice_pod_old),1,14))
      .select(col(RcuPodPSchema.t_codice_pod), dsTdeVulnP("*"))
      .persist()


    dfTabPod.unpersist()

    val dftabF = dfTabB
      .alias(tabB)
      .join(
        dsRcuClienteFinaleP.alias(p),
        dfTabB(tdeVulnPSchema.n_id_cliente) === dsRcuClienteFinaleP(RcuClienteFinalePSchema.n_id_cliente),
        "inner"
      )
      .select(dsRcuClienteFinaleP(RcuClienteFinalePSchema.t_cf), dfTabB("*"))
      .persist()

    dfTabB.unpersist()

    val forniture_elettriche2 = forniture_elettriche_1Join
    .alias(AA)
      .join(
        dftabF.alias(tabF),
        forniture_elettriche_1Join(fornitureElettricheSchema.t_cf) === dftabF(RcuClienteFinalePSchema.t_cf) &&
          forniture_elettriche_1Join(fornitureElettricheSchema.codice_pod) === dftabF(RcuPodPSchema.t_codice_pod),
        "left"
      )
      .withColumn(fornitureElettricheSchema.isvulnerabile,when(dftabF(RcuClienteFinalePSchema.t_cf).isNull, "N").
        otherwise("Y"))
      .select(
        forniture_elettriche_1Join("*"),
        col(fornitureElettricheSchema.isvulnerabile)
//        when(dftabF(RcuClienteFinalePSchema.t_cf).isNull, "N").
//          otherwise("Y").as(fornitureElettricheSchema.isvulnerabile)
      ).persist()

    forniture_elettriche_1Join.unpersist()
    dftabF.unpersist()


    val dfB = dsRcuCodiceOffertaP
      .filter(col(rcuCodiceOffertaPSchema.d_data_fine).isNull) // Filter where d_data_fine is null
      .groupBy(rcuCodiceOffertaPSchema.n_id_fornitura, rcuCodiceOffertaPSchema.t_codice_offerta) // Group by n_id_fornitura and t_codice_offerta
      .agg(
        max(rcuCodiceOffertaPSchema.d_aggiornamento).as(max_d_aggiornamento) // Compute max(d_aggiornamento)
      ).persist()


    val forniture_elettriche = forniture_elettriche2
      .alias(A)
      .join(
        dfB.alias(B),
        forniture_elettriche2(fornitureElettricheSchema.codice_fornitura) === dfB(rcuCodiceOffertaPSchema.n_id_fornitura),
        "left"
      )
      .select(
        col( A + DOT + "*"),
        coalesce(dfB(rcuCodiceOffertaPSchema.t_codice_offerta), lit("")).as(fornitureElettricheSchema.codice_offerta) // Use NVL equivalent with coalesce
      ).selectExpr(fornitureElettricheSchema.getValues:_*)
      .as[fornitureElettricheModel]

    dfB.unpersist()
    forniture_elettriche2.unpersist()

    forniture_elettriche
  }

}
