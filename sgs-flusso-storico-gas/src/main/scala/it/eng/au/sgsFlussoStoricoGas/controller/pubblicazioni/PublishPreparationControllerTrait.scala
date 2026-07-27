package it.eng.au.sgsFlussoStoricoGas.controller.pubblicazioni

import it.eng.au.sgsFlussoStoricoGas.model.PubListValidatedModel
import it.eng.au.sgsFlussoStoricoGas.schema.aggregazione.AggregatoreInfoDettSchema
import it.eng.au.sgsFlussoStoricoGas.schema.perimetro.SgsPerimetroSchema
import it.eng.au.sgsFlussoStoricoGas.schema.pubblicazione.{PubblicazioneInfoDettSchema, PubblicazioneInfoSchema, SgsReportSchema, XmlOutputSchema}
import it.eng.au.sgsFlussoStoricoGas.utility.constants.StatoAggregazione
import it.eng.au.sgsFlussoStoricoGas.utility.environment.Environment
import org.apache.spark.sql.functions.{col, concat, concat_ws, lit, lpad, md5, month, year}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}

trait PublishPreparationControllerTrait {

  val tipoFlusso = ""
  val tipoPratica = ""
  val nomeServizio = ""

  def getPubblicazioneInfoTables(AggregatoDF: DataFrame, perimetroDF: DataFrame, pubList: List[PubListValidatedModel]): DataFrame = {
    val SQLContext = Environment.getSpark.sqlContext
    import SQLContext.implicits._

    val pubDF = pubList
      .toDS()
      .flatMap { case PubListValidatedModel(piva, _, _, cods, path, zipFileName, index, xsdValidationResult) =>
        cods.split(",").map(cod => (piva, cod, path, zipFileName, index, xsdValidationResult))
      }.toDF("piva_utente", "cod_pdr", "path", "zipFileName", "index", "xsd_validation_result")

    AggregatoDF
      .filter(col(AggregatoreInfoDettSchema.t_stato_dett)===StatoAggregazione.OK.toString)
      .join(pubDF, Seq(AggregatoreInfoDettSchema.cod_pdr.toString), "inner")
      .join(perimetroDF.selectExpr(SgsPerimetroSchema.n_id_pratica.toString, SgsPerimetroSchema.n_id_pdr.toString), Seq(AggregatoreInfoDettSchema.n_id_pratica.toString), "left")
      .filter(col(AggregatoreInfoDettSchema.tipo_flusso) === tipoFlusso)
      .filter(col(AggregatoreInfoDettSchema.tipo_pratica) === tipoPratica)
      .filter(col(AggregatoreInfoDettSchema.nome_servizio) === nomeServizio)
      .withColumn(PubblicazioneInfoSchema.nome_flusso, lit("SGS"))
      .withColumn(PubblicazioneInfoSchema.data_pubblicazione, lit(Environment.startDateTime.toLocalDate.toString))
      .withColumn(PubblicazioneInfoSchema.seq, col("index").cast(IntegerType))
      .withColumnRenamed("zipFileName", PubblicazioneInfoSchema.nome_file)
      .withColumnRenamed("xsd_validation_result", PubblicazioneInfoSchema.xsd_validated)
      .withColumnRenamed(AggregatoreInfoDettSchema.anno_mese_comp, PubblicazioneInfoSchema.anno_mese_decorrenza)
      .withColumnRenamed(AggregatoreInfoDettSchema.nome_servizio, PubblicazioneInfoSchema.tipo_dest)
      .withColumnRenamed(AggregatoreInfoDettSchema.execution_id, PubblicazioneInfoSchema.execution_id_agg_sgs)
      .withColumn(PubblicazioneInfoSchema.execution_id, lit(Environment.executionId))
      .withColumn(PubblicazioneInfoSchema.id_pubblicazione_info, md5(concat(PubblicazioneInfoSchema.getValues.diff(Seq(PubblicazioneInfoSchema.id_pubblicazione_info.toString)).map(col): _*)).cast(StringType))
  }

  def getPubblicazioneInfo(AggregatoDF: DataFrame, perimetroDF: DataFrame, pubList: List[PubListValidatedModel]): DataFrame = {
    getPubblicazioneInfoTables(AggregatoDF, perimetroDF, pubList)
      .selectExpr(PubblicazioneInfoSchema.getValues:_*)
      .distinct
  }

  def getPubblicazioneInfoDett(AggregatoDF: DataFrame, perimetroDF: DataFrame, pubList: List[PubListValidatedModel]): DataFrame = {
    getPubblicazioneInfoTables(AggregatoDF, perimetroDF, pubList)
      .selectExpr(PubblicazioneInfoDettSchema.getValues: _*)
      .distinct
  }

  def getXmlOutputDF(AggregatoDF: DataFrame): DataFrame = {
    AggregatoDF
      .filter(col(AggregatoreInfoDettSchema.t_stato_dett)===StatoAggregazione.OK.toString)
      .filter(col(AggregatoreInfoDettSchema.tipo_flusso) === tipoFlusso)
      .filter(col(AggregatoreInfoDettSchema.tipo_pratica) === tipoPratica)
      .filter(col(AggregatoreInfoDettSchema.nome_servizio) === nomeServizio)
      .withColumnRenamed(AggregatoreInfoDettSchema.piva_utente_dest, XmlOutputSchema.piva_utente)
      .withColumnRenamed(AggregatoreInfoDettSchema.classe_mis_int, XmlOutputSchema.gruppo_mis_int)
      .withColumnRenamed(AggregatoreInfoDettSchema.coeff_corr, XmlOutputSchema.coeff_cor)
      .withColumnRenamed(AggregatoreInfoDettSchema.prelievo_aggregato, XmlOutputSchema.prel_aggregato)
      .withColumnRenamed(AggregatoreInfoDettSchema.anno_mese_comp, XmlOutputSchema.mese_anno_decorr)
      .withColumn(XmlOutputSchema.mese_anno_pub, concat(year(col(AggregatoreInfoDettSchema.d_data_decorrenza)), lpad(month(col(AggregatoreInfoDettSchema.d_data_decorrenza)), 2, "0")).cast(StringType))
      .withColumn(XmlOutputSchema.mese_anno, concat_ws("/", col(AggregatoreInfoDettSchema.anno_mese).substr(5,2), col(AggregatoreInfoDettSchema.anno_mese).substr(1,4)))
      .selectExpr(XmlOutputSchema.getValues:_*)
  }

  def getSgsReportDF(AggregatoDF: DataFrame, PubblicazioneInfoDF: DataFrame, PubblicazioneInfoDettDF: DataFrame):DataFrame = {
    val groupingCols = SgsReportSchema.getValues.diff(SgsReportSchema.prelievo_aggregato.toString).map(col)

    PubblicazioneInfoDF
      .join(PubblicazioneInfoDettDF, Seq(PubblicazioneInfoSchema.id_pubblicazione_info.toString), "inner")
      .join(AggregatoDF.filter(col(AggregatoreInfoDettSchema.t_stato_dett)===StatoAggregazione.OK.toString).drop(AggregatoreInfoDettSchema.piva_utente_dest.toString), Seq(PubblicazioneInfoDettSchema.n_id_pratica.toString), "left")
      .filter(col(AggregatoreInfoDettSchema.tipo_flusso) === tipoFlusso)
      .filter(col(AggregatoreInfoDettSchema.tipo_pratica) === tipoPratica)
      .filter(col(AggregatoreInfoDettSchema.nome_servizio) === nomeServizio)
      .withColumnRenamed(AggregatoreInfoDettSchema.d_data_decorrenza, SgsReportSchema.data_decorrenza_pratica)
      .withColumnRenamed(AggregatoreInfoDettSchema.tipo_pratica, SgsReportSchema.tipologia_pratica)
      .withColumnRenamed(PubblicazioneInfoSchema.path, SgsReportSchema.pathfile)
      .withColumn(SgsReportSchema.piva_udb, col(PubblicazioneInfoSchema.piva_utente_dest))
      .withColumn(SgsReportSchema.piva_udd, col(PubblicazioneInfoSchema.piva_utente_dest))
      .withColumn(SgsReportSchema.piva_id, col(AggregatoreInfoDettSchema.piva_distr))
      .withColumn(SgsReportSchema.piva_id, col(AggregatoreInfoDettSchema.piva_distr))
      .groupBy(groupingCols:_*)
      .sum(AggregatoreInfoDettSchema.prelievo_aggregato.toString).alias(SgsReportSchema.prelievo_aggregato.toString)
      .selectExpr(SgsReportSchema.getValues:_*)
      .distinct
  }

}
