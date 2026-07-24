package it.au.misure.ingestionMisureGasUnico.flow.standard.r

import it.au.misure.ingestionMisureGasUnico.model.schema.{IGMGSchema, IGMRSchema, SchemaEnum}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RMLSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.r.RettificaXMLSchema._
import RMLSchema.{annomese, cod_flusso, cod_pdr, cod_servizio, data_racc}
import it.au.misure.ingestionMisureGasUnico.model.GasXmlMetadata
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.validate.ReportEsitoPDRMessage
import it.au.misure.ingestionMisureGasUnico.utility.Constants.BLOCCANTE
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment
import it.au.misure.ingestionMisureGasUnico.validate.CheckAmmissibilitaPDRRules
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel

import scala.xml.{Node, XML}

object RMLStandardFlow extends RettificaFlow {
  override val schema: SchemaEnum = RMLSchema

  override val hiveTableName: String = "prt_cmg_rml_p"
  val igmgHiveTableName: String = "prt_cmg_igmg_p"
  val igmrHiveTableName: String = "prt_cmg_igmr_p"

  override def flowName: String = "RML"

  override val partitioningColumns: List[String] = List(
    annomese.toString
  )

  override val renamedColumns: Map[String, String] = Map(
    cod_flusso.toString -> cod_servizio.toString
  )


  override def validate(inputRdd: RDD[GasXmlMetadata]) /*(implicit sc: SparkContext, sqlContext: SQLContext)*/ : (RDD[(GasXmlMetadata, List[ReportEsitoPDRMessage])], RDD[GasXmlMetadata]) = {
    val checkAmm = Environment.getSpark.sparkContext.broadcast(new CheckAmmissibilitaPDRRules)

    val pdrRDD = inputRdd.flatMap(gasXmlMetada => {
      (gasXmlMetada.xmlNode \\ MisuraXMLSchema.FlussoMisure \\ MisuraXMLSchema.DatiPdr).toList.map(pdr => (gasXmlMetada.copy(xmlNode = null), pdr))
    }).repartition(inputRdd.partitions.length)

    val prtIgmg = Environment.getSpark.sqlContext.table(s"$hiveDatabaseName.$igmgHiveTableName")
    val prtIgmr = Environment.getSpark.sqlContext.table(s"$hiveDatabaseName.$igmrHiveTableName")

    val pdrWithExtraMetaRdd = getPdrExtraMetadata(pdrRDD.map { case (metadata, node) => (node, metadata) }, prtIgmg, prtIgmr)
      .map { case (node, metadata) => (metadata, node) }

    val xmlWithMessages = pdrWithExtraMetaRdd.map({ case (meta, pdr) =>
      (meta.file.getPath, (meta, checkAmm.value.check(pdr, meta)))
    }).groupByKey()
      .map({ case (filename, iterable) => (iterable.toList.head._1, iterable.toList.map(_._2)) })
      .map({ case (meta, messageList) => (meta.copy(xmlNode = XML.loadFile(meta.file)), messageList) })
      .persist(StorageLevel.MEMORY_AND_DISK)

    val outputRddData = xmlWithMessages.map({ case (gasXmlMetada, messages) =>
      val ammissibilitaMap = messages.map(message => (message.pdr, message.bloccante)).toMap
      gasXmlMetada.copy(ammissibilita = ammissibilitaMap)
    })

    (xmlWithMessages, outputRddData)
  }

  def getPdrExtraMetadata(rddPdrMeta: RDD[(Node, GasXmlMetadata)], prtIgmg: DataFrame, prtIgmr: DataFrame): RDD[(Node, GasXmlMetadata)] = {
    val prtCmgIgmg = prtIgmg
      .filter(col(IGMGSchema.ammissibilita) =!= BLOCCANTE)
      .select(IGMGSchema.cod_pdr, IGMGSchema.data_misura)
      .withColumn(IGMGSchema.ammissibilita, lit("RELATIVO_IGMG_PRESENTE"))
      .rdd
      .map(row => (row.getAs[String](IGMGSchema.cod_pdr), row.getAs[String](IGMGSchema.data_misura), row.getAs[String](IGMGSchema.ammissibilita)))

    val prtCmgIgmgMapped: RDD[((String, String), String)] = prtCmgIgmg
      .map { case (cod_pdr, data_misura, igmgMatch) => ((cod_pdr, data_misura), igmgMatch) }

    val prtCmgIgmr = prtIgmr
      .filter(col(IGMRSchema.ammissibilita) =!= BLOCCANTE && col(IGMRSchema.mot_ret_lett) === "2")
      .select(IGMRSchema.cod_pdr, IGMRSchema.data_misura)
      .withColumn(IGMRSchema.ammissibilita, lit("RELATIVO_IGMR_PRESENTE"))
      .rdd
      .map(row => (row.getAs[String](IGMRSchema.cod_pdr), row.getAs[String](IGMRSchema.data_misura), row.getAs[String](IGMRSchema.ammissibilita)))

    val prtCmgIgmrMapped: RDD[((String, String), String)] =
      prtCmgIgmr
        .map { case (cod_pdr, data_misura, igmrMatch) => ((cod_pdr, data_misura), igmrMatch) }

    val rdd = rddPdrMeta.map({ case (pdrNode, meta) => (((pdrNode \ cod_pdr).text, (pdrNode \ DatiLetturaRett \ data_racc).text), (pdrNode, meta.copy(xmlNode = null))) })

    val joinedRdd = rdd
      .leftOuterJoin(prtCmgIgmgMapped)
      .map {
        case (_, ((node, metadata), igmgMatchOpt)) =>
          val updatedMetadata = metadata.copy(igmgMatch = igmgMatchOpt.getOrElse(""))
          (node, updatedMetadata)
      }
      .map({ case (pdrNode, meta) => (((pdrNode \ cod_pdr).text, (pdrNode \ DatiLetturaRett \ data_racc).text), (pdrNode, meta.copy(xmlNode = null))) })
      .leftOuterJoin(prtCmgIgmrMapped)
      .map {
        case (_, ((node, metadata), igmrMatchOpt)) =>
          val updatedMetadata = metadata.copy(igmrMatch = igmrMatchOpt.getOrElse(""))
          (node, updatedMetadata)
      }

    joinedRdd
  }
}
