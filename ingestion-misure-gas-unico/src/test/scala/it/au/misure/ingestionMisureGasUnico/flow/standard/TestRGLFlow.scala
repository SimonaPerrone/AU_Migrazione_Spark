package it.au.misure.ingestionMisureGasUnico.flow.standard

import it.au.misure.ingestionMisureGasUnico.flow.standard.r.RGLStandardFlow.{getPdrExtraMetadata, loadData}
import it.au.misure.ingestionMisureGasUnico.model.schema.standard.m.MisuraXMLSchema
import it.au.misure.ingestionMisureGasUnico.model.schema.{IGMGSchema, IGMRSchema}
import it.au.misure.ingestionMisureGasUnico.utility.EnvironmentSparkTest
import it.au.misure.ingestionMisureGasUnico.utility.environment.Environment

class TestRGLFlow extends EnvironmentSparkTest {
  def testGetPdrExtraMetadata(): Unit = {
    val sqlContext = Environment.getSpark.sqlContext
    import sqlContext.implicits._

    val prtIgmg = Environment.getSpark.sparkContext.parallelize(Seq(
      ("08450000047322", "12/09/2025", "OK"),
      ("08450000047322", "30/09/2025", "OK"),
      ("08450000047322", "25/09/2025", "OK")
    ))
      .toDF(IGMGSchema.cod_pdr, IGMGSchema.data_misura, IGMGSchema.ammissibilita)

    val prtIgmr = Environment.getSpark.sparkContext.parallelize(Seq(
      ("08450000047322", "12/09/2025", "OK", "2"),
      ("08450000047322", "30/09/2025", "OK", "2"),
      ("08450000047322", "25/09/2025", "OK", "2")
    ))
      .toDF(IGMRSchema.cod_pdr, IGMRSchema.data_misura, IGMRSchema.ammissibilita, IGMRSchema.mot_ret_lett)

    val inputRdd = loadData()

    val pdrWithMetaRdd = inputRdd.flatMap(gasXmlMetada => {
      (gasXmlMetada.xmlNode \\ MisuraXMLSchema.FlussoMisure \\ MisuraXMLSchema.DatiPdr).toList.map(pdr => (gasXmlMetada.copy(xmlNode = null), pdr))
    })

    val pdrWithExtraMetaRdd = getPdrExtraMetadata(pdrWithMetaRdd.map {case (metadata, node) => (node, metadata)}, prtIgmg, prtIgmr)

    pdrWithExtraMetaRdd.take(10).foreach(println)
  }
}
