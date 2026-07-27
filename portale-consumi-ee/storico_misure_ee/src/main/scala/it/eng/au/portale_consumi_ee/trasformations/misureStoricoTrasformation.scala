package it.eng.au.portale_consumi_ee.trasformations

import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.misure.{MisureStoricF2Model, MisureStoricFModel, MisureStoricModel, MisureStoricNoraModel}
import it.eng.au.portale_consumi_ee.model.mongodbs.FornitureElettricheTmpModel
import it.eng.au.portale_consumi_ee.schema.misure.{MisureStoricF2Schema, MisureStoricFSchema, MisureStoricNoraSchema, MisureStoricSchema}
import it.eng.au.portale_consumi_ee.schema.mongodbs.FornitureElettricheTmpSchema
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit, substring, when}

object misureStoricoTrasformation {

//  //todo to modify Environment. need to be Environment for misure storiche
  val spark = EnvironmentMisure.getSpark
  import spark.implicits._

  def misureStoricOraDefinition(dsMisureStoric: Dataset[MisureStoricModel],dsFornitureElettriche: Dataset[FornitureElettricheTmpModel])(implicit spark: SparkSession) : Dataset[MisureStoricF2Model] = {

    def dsMisureStoricOra = dsMisureStoric.join(dsFornitureElettriche,
      col(MisureStoricSchema.pod) === col(FornitureElettricheTmpSchema.codice_pod),
      "inner"
    )
      .filter(col(MisureStoricSchema.data_lettura) >= col(FornitureElettricheTmpSchema.inizio)
        and col(MisureStoricSchema.data_lettura) <= col(FornitureElettricheTmpSchema.fine))
      .select(
        col(FornitureElettricheTmpSchema.cf_piva),
        col(MisureStoricSchema.pod),
        col(MisureStoricSchema.data_lettura_str),
        col(MisureStoricSchema.data_ricezione_str),
        col(MisureStoricSchema.descr_motivazione),
        col(MisureStoricSchema.eaf1),
        col(MisureStoricSchema.eaf2),
        col(MisureStoricSchema.eaf3),
        col(MisureStoricSchema.eaf4),
        col(MisureStoricSchema.eaf5),
        col(MisureStoricSchema.eaf6),
        col(MisureStoricSchema.ea),
        col(MisureStoricSchema.er),
        col(MisureStoricSchema.descr_tipoflusso),
        col(MisureStoricSchema.annomese),
        col(MisureStoricSchema.data_lettura)
      )
      .withColumnRenamed(MisureStoricSchema.data_lettura,MisureStoricFSchema.data_lettura_num)
      .withColumnRenamed(MisureStoricSchema.data_lettura_str,MisureStoricFSchema.data_lettura)
      .withColumnRenamed(MisureStoricSchema.data_ricezione_str,MisureStoricFSchema.data_ricezione)
      .withColumnRenamed(MisureStoricSchema.descr_motivazione,MisureStoricFSchema.motivazione)
      .withColumnRenamed(MisureStoricSchema.eaf1,MisureStoricFSchema.lettura_f1)
      .withColumnRenamed(MisureStoricSchema.eaf2,MisureStoricFSchema.lettura_f2)
      .withColumnRenamed(MisureStoricSchema.eaf3,MisureStoricFSchema.lettura_f3)
      .withColumnRenamed(MisureStoricSchema.eaf4,MisureStoricFSchema.lettura_f4)
      .withColumnRenamed(MisureStoricSchema.eaf5,MisureStoricFSchema.lettura_f5)
      .withColumnRenamed(MisureStoricSchema.eaf6,MisureStoricFSchema.lettura_f6)
      .withColumnRenamed(MisureStoricSchema.ea,MisureStoricFSchema.ea)
      .withColumnRenamed(MisureStoricSchema.er,MisureStoricFSchema.er)
      .withColumnRenamed(MisureStoricSchema.descr_tipoflusso,MisureStoricFSchema.tipo_flusso)
      .withColumnRenamed(MisureStoricSchema.annomese,MisureStoricFSchema.annomese_riferimento)
      .withColumn(MisureStoricFSchema.lettura_monoraria,lit(null))
      .withColumn(MisureStoricFSchema.cod_pod,substring(col(MisureStoricSchema.pod), 11, 2))
      .withColumn(MisureStoricFSchema.is_mis_oraria,lit("1"))
      .selectExpr(MisureStoricF2Schema.getValues:_*)
      .as[MisureStoricF2Model]

    dsMisureStoricOra
  }

  def misureStoricNoraDefinition(dsMisureStoric: Dataset[MisureStoricNoraModel],dsFornitureElettriche: Dataset[FornitureElettricheTmpModel])(implicit spark: SparkSession) : Dataset[MisureStoricF2Model] = {

    val fornitureTmp = "fornitureTmp"
    val storicNora = "storicNora"
    def dsMisureStoricNora = dsMisureStoric.as(storicNora)
      .join(dsFornitureElettriche.as(fornitureTmp),
      col(MisureStoricNoraSchema.pod) === col(FornitureElettricheTmpSchema.codice_pod),
      "inner"
    )
      .filter(col(MisureStoricNoraSchema.data_lettura) >= col(FornitureElettricheTmpSchema.inizio)
        and col(MisureStoricNoraSchema.data_lettura) <= col(FornitureElettricheTmpSchema.fine))
      .select(
        col(FornitureElettricheTmpSchema.cf_piva),
        col(MisureStoricNoraSchema.pod),
        col(MisureStoricNoraSchema.data_lettura_str),
        col(MisureStoricNoraSchema.data_ricezione_str),
        col(MisureStoricNoraSchema.descr_motivazione),
        col(MisureStoricNoraSchema.eaf1),
        col(MisureStoricNoraSchema.eam),
        col(MisureStoricNoraSchema.eaf2),
        col(MisureStoricNoraSchema.eaf3),
        col(MisureStoricNoraSchema.eaf4),
        col(MisureStoricNoraSchema.eaf5),
        col(MisureStoricNoraSchema.eaf6),
        col(MisureStoricNoraSchema.ea),
        col(MisureStoricNoraSchema.er),
        col(MisureStoricNoraSchema.descr_tipoflusso),
        col(MisureStoricNoraSchema.annomese),
        col(MisureStoricNoraSchema.data_lettura)
      )

      def finalDf = dsMisureStoricNora
      .withColumnRenamed(MisureStoricNoraSchema.data_lettura,MisureStoricF2Schema.data_lettura_num)
      .withColumnRenamed(MisureStoricNoraSchema.data_lettura_str,MisureStoricF2Schema.data_lettura)
      .withColumnRenamed(MisureStoricNoraSchema.data_ricezione_str,MisureStoricF2Schema.data_ricezione)
      .withColumnRenamed(MisureStoricNoraSchema.descr_motivazione,MisureStoricF2Schema.motivazione)
      .withColumnRenamed(MisureStoricNoraSchema.eaf1,MisureStoricF2Schema.lettura_f1)
      .withColumnRenamed(MisureStoricNoraSchema.eaf2,MisureStoricF2Schema.lettura_f2)
      .withColumnRenamed(MisureStoricNoraSchema.eaf3,MisureStoricF2Schema.lettura_f3)
      .withColumnRenamed(MisureStoricNoraSchema.eaf4,MisureStoricF2Schema.lettura_f4)
      .withColumnRenamed(MisureStoricNoraSchema.eaf5,MisureStoricF2Schema.lettura_f5)
      .withColumnRenamed(MisureStoricNoraSchema.eaf6,MisureStoricF2Schema.lettura_f6)
      .withColumnRenamed(MisureStoricNoraSchema.ea,MisureStoricF2Schema.ea)
      .withColumnRenamed(MisureStoricNoraSchema.er,MisureStoricF2Schema.er)
      .withColumnRenamed(MisureStoricNoraSchema.descr_tipoflusso,MisureStoricF2Schema.tipo_flusso)
      .withColumnRenamed(MisureStoricNoraSchema.annomese,MisureStoricF2Schema.annomese_riferimento)
      .withColumn(MisureStoricF2Schema.lettura_monoraria, when(coalesce(col(MisureStoricF2Schema.lettura_f1), lit(0)) =!= 0, lit(null).cast("double"))
        .otherwise(col(MisureStoricNoraSchema.eam)))
      .withColumn(MisureStoricFSchema.cod_pod,substring(col(MisureStoricNoraSchema.pod), 11, 2))
      .withColumn(MisureStoricF2Schema.is_mis_oraria,lit("0"))
        .selectExpr(MisureStoricF2Schema.getValues:_*)
      .as[MisureStoricF2Model]

    finalDf
  }

}
