package it.eng.au.portale_consumi_ee.trasformations

import it.eng.au.portale_consumi_ee.common.schema.mongodbs.fornitureElettricheSchema
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{ConsultazioneModel, MisureStoricF2Model, MisureStoricModel, MisureStoricNoraModel, misureStoricF2ErcEriModel, misureStoricNoraErcEriModel}
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.schema.misure.{ConsultazioneSchema, MisureStoricF2Schema, MisureStoricFSchema, MisureStoricNoraSchema, MisureStoricSchema, misureStoricF2ErcEriSchema, misureStoricNoraErcEriSchema}
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureElettricheTmpSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}

object misureStoricoErcEriTrasformation {

//  //todo to modify Environment. need to be Environment for misure storiche
  val spark = EnvironmentMisure.getSpark
  import spark.implicits._

  def misureStoricF2Prepared(
                              dsMisureStoricF2: Dataset[MisureStoricF2Model],
                              annomese:Integer
                            )(implicit spark: SparkSession) : Dataset[MisureStoricF2Model] =
  {
    dsMisureStoricF2.filter(col(MisureStoricF2Schema.annomese_riferimento) === annomese )

  }

  def misureStoricF2ErcEriPrepared(
                              dsMisureStoricErcEriF2: Dataset[misureStoricF2ErcEriModel],
                                annomese:Integer
                            )(implicit spark: SparkSession) : Dataset[misureStoricF2ErcEriModel] =
  {

    dsMisureStoricErcEriF2.filter(col(misureStoricF2ErcEriSchema.annomese_riferimento) === annomese )
  }


  def consultazioneDefinition(
                                  dsMisureStoricF2: Dataset[MisureStoricF2Model],
                                  dsMisureStoricF2ErcEri: Dataset[misureStoricF2ErcEriModel]
                                )(implicit spark: SparkSession) : Dataset[ConsultazioneModel] =
  {
    val data_lettura_erc = "data_lettura_erc"
    val annomese_riferimento_erc = "annomese_riferimento_erc"
    val data_ricezione_erc = "data_ricezione_erc"
    val tipo_flusso_erc = "tipo_flusso_erc"
    val pod_erc = "pod_erc"


    def dsMisureStoricF2Prepared = dsMisureStoricF2
      .withColumn(MisureStoricF2Schema.ea, when(trim(coalesce(col(MisureStoricF2Schema.ea), lit(""))) === "", repeat(lit(";"), 95)).otherwise(col(MisureStoricF2Schema.ea)))
      .withColumn(MisureStoricF2Schema.er, when(trim(coalesce(col(MisureStoricF2Schema.er), lit(""))) === "", repeat(lit(";"), 95)).otherwise(col(MisureStoricF2Schema.er)))
      .select(
        col(MisureStoricF2Schema.cf_piva),
        col(MisureStoricF2Schema.annomese_riferimento),
        col(MisureStoricF2Schema.data_lettura),
        col(MisureStoricF2Schema.data_ricezione),
        col(MisureStoricF2Schema.tipo_flusso),
        col(MisureStoricF2Schema.pod),
        col(MisureStoricF2Schema.motivazione),
        col(MisureStoricF2Schema.lettura_monoraria),
        col(MisureStoricF2Schema.lettura_f1),
        col(MisureStoricF2Schema.lettura_f2),
        col(MisureStoricF2Schema.lettura_f3),
        col(MisureStoricF2Schema.lettura_f4),
        col(MisureStoricF2Schema.lettura_f5),
        col(MisureStoricF2Schema.lettura_f6),
        col(MisureStoricF2Schema.ea),
        col(MisureStoricF2Schema.er),
        col(MisureStoricF2Schema.cod_pod)
      )

    def dsMisureStoricF2ErcEriPrepared = dsMisureStoricF2ErcEri
      .withColumn(misureStoricF2ErcEriSchema.erc, when(trim(coalesce(col(misureStoricF2ErcEriSchema.erc), lit(""))) === "", repeat(lit("a;"), 100)).otherwise(col(misureStoricF2ErcEriSchema.erc)))
      .withColumn(misureStoricF2ErcEriSchema.eri, when(trim(coalesce(col(misureStoricF2ErcEriSchema.eri), lit(""))) === "", repeat(lit("a;"), 100)).otherwise(col(misureStoricF2ErcEriSchema.eri)))
      .withColumn(data_lettura_erc,col(misureStoricF2ErcEriSchema.data_lettura_str))
      .withColumn(pod_erc,col(misureStoricF2ErcEriSchema.pod))
      .withColumn(annomese_riferimento_erc, col(misureStoricF2ErcEriSchema.annomese_riferimento))
      .withColumn(data_ricezione_erc,col(misureStoricF2ErcEriSchema.data_ricezione))
      .withColumn(tipo_flusso_erc,col(misureStoricF2ErcEriSchema.tipo_flusso))
      .select(
        col(misureStoricF2ErcEriSchema.erc),
        col(misureStoricF2ErcEriSchema.eri),
        col(data_lettura_erc),
        col(pod_erc),
        col(annomese_riferimento_erc),
        col(data_ricezione_erc),
        col(tipo_flusso_erc)
      )

    val joinDf = dsMisureStoricF2Prepared.join(dsMisureStoricF2ErcEriPrepared
      , col(MisureStoricF2Schema.pod) === col(pod_erc) &&
        col(MisureStoricF2Schema.annomese_riferimento) === col(annomese_riferimento_erc) &&
        col(MisureStoricF2Schema.data_lettura) === col(data_lettura_erc) &&
        col(MisureStoricF2Schema.data_ricezione) === col(data_ricezione_erc) &&
        upper(regexp_replace(col(MisureStoricF2Schema.tipo_flusso)," ","")) === upper(regexp_replace(col(tipo_flusso_erc)," ",""))
      ,"left")
      .withColumn(
        misureStoricF2ErcEriSchema.erc,
        when(col(misureStoricF2ErcEriSchema.erc).isNull, "").otherwise(col(misureStoricF2ErcEriSchema.erc))
      )
      .withColumn(
        misureStoricF2ErcEriSchema.eri,
        when(col(misureStoricF2ErcEriSchema.eri).isNull, "").otherwise(col(misureStoricF2ErcEriSchema.eri))
      )

    joinDf.selectExpr(ConsultazioneSchema.getValues:_*).as[ConsultazioneModel]
  }

  def misureStoricNoraF2ErcEriPrepared(
                                        ds: Dataset[misureStoricNoraErcEriModel],
                                        dsFornitureElettriche: Dataset[FornitureElettricheTmpModel]
                                      )(implicit spark: SparkSession) : Dataset[misureStoricF2ErcEriModel] = {

    val misureWithCodPod = ds
      .withColumn(misureStoricF2ErcEriSchema.cod_pod, substring(col(misureStoricNoraErcEriSchema.pod), 11, 2))

    val joinedDf = misureWithCodPod
      .join(dsFornitureElettriche, misureWithCodPod(misureStoricNoraErcEriSchema.pod) === dsFornitureElettriche(FornitureElettricheTmpSchema.codice_pod))
      .filter(col(misureStoricNoraErcEriSchema.data_lettura) >= col(FornitureElettricheTmpSchema.inizio) && col(misureStoricNoraErcEriSchema.data_lettura) <= col(FornitureElettricheTmpSchema.fine))
      .select(
        dsFornitureElettriche(FornitureElettricheTmpSchema.cf_piva),
        misureWithCodPod(misureStoricNoraErcEriSchema.pod),
        misureWithCodPod(misureStoricNoraErcEriSchema.data_lettura_str),
        misureWithCodPod(misureStoricNoraErcEriSchema.data_ricezione_str).as(misureStoricF2ErcEriSchema.data_ricezione),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_erc_f1),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_erc_f2),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_erc_f3),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_erc_f4),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_erc_f5),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_erc_f6),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_eri_f1),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_eri_f2),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_eri_f3),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_eri_f4),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_eri_f5),
        misureWithCodPod(misureStoricNoraErcEriSchema.lettura_eri_f6),
        misureWithCodPod(misureStoricNoraErcEriSchema.erc),
        misureWithCodPod(misureStoricNoraErcEriSchema.eri),
        misureWithCodPod(misureStoricNoraErcEriSchema.tipo_flusso),
        misureWithCodPod(misureStoricNoraErcEriSchema.annomese).cast(IntegerType).alias(MisureStoricF2Schema.annomese_riferimento),
        misureWithCodPod(misureStoricNoraErcEriSchema.data_lettura),
        misureWithCodPod(misureStoricF2ErcEriSchema.cod_pod),
        lit("0").alias(misureStoricF2ErcEriSchema.is_mis_oraria)
      )

    joinedDf.selectExpr(misureStoricF2ErcEriSchema.getValues:_*).as[misureStoricF2ErcEriModel]
  }

}
