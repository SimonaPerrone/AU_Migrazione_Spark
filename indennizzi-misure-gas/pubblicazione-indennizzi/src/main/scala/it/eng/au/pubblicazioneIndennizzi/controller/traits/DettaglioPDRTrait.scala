package it.eng.au.pubblicazioneIndennizzi.controller.traits

import it.eng.au.indennizziMisureGasCommon.schema.DettaglioPdrSchema
import it.eng.au.pubblicazioneIndennizzi.dao.Dao
import it.eng.au.pubblicazioneIndennizzi.dao.cig.DettaglioPdrDao
import it.eng.au.pubblicazioneIndennizzi.schema.DETTAGLIO_PDR_IZGOutputSchema
import org.apache.spark.sql.functions.{col, explode_outer, regexp_extract, split}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.immutable.ListMap

trait DettaglioPDRTrait extends RunnableAggregator {
  override def daoTableName: Dao = new DettaglioPdrDao()
  override val operationName: String = "DETTAGLIO_PDR_IZG"
  override val aggregatoColumns: ListMap[String, String] = ListMap(
    DettaglioPdrSchema.id_indennizzo.toString -> DETTAGLIO_PDR_IZGOutputSchema.ID_INDENNIZZO.toString,
    DettaglioPdrSchema.piva_id.toString -> DETTAGLIO_PDR_IZGOutputSchema.PIVA_ID.toString,
    DettaglioPdrSchema.rag_soc_id.toString -> DETTAGLIO_PDR_IZGOutputSchema.RAG_SOC_ID.toString,
    DettaglioPdrSchema.piva_udd.toString -> DETTAGLIO_PDR_IZGOutputSchema.PIVA_UDD.toString,
    DettaglioPdrSchema.rag_soc_udd.toString -> DETTAGLIO_PDR_IZGOutputSchema.RAG_SOC_UDD.toString,
    DettaglioPdrSchema.annomese.toString -> DETTAGLIO_PDR_IZGOutputSchema.AAAAMM.toString,
    DettaglioPdrSchema.pdr.toString -> DETTAGLIO_PDR_IZGOutputSchema.PDR_G.toString,
    DettaglioPdrSchema.nome_file.toString -> DETTAGLIO_PDR_IZGOutputSchema.NOME_FILE.toString
  )
  override val csvFields: List[String] = aggregatoColumns.values.toList

  override def getAggregato(df: DataFrame): DataFrame = {
    val orderedSelectList = aggregatoColumns.values.toList

    var aggDF = df.filter(fileSpecificFilterExpression)
      .withColumn(DettaglioPdrSchema.nome_file, explode_outer(split(col(DettaglioPdrSchema.nome_file), ",")))
      .withColumn(DettaglioPdrSchema.nome_file, regexp_extract(col(DettaglioPdrSchema.nome_file), "(\\/[0-9]{4}){2}\\/.*\\..*", 0))

    aggregatoColumns.foreach({ case (tableName, fileName) =>
      aggDF = aggDF.withColumnRenamed(tableName, fileName)
    })

    aggDF.selectExpr(orderedSelectList: _*)
  }

  override def fileSpecificFilterExpression: Column = (col(DettaglioPdrSchema.piva_id).isNotNull
    and col(DettaglioPdrSchema.piva_udd).isNotNull
    and col(DettaglioPdrSchema.annomese).isNotNull
    and col(DettaglioPdrSchema.piva_id) =!= ""
    and col(DettaglioPdrSchema.piva_udd) =!= ""
    and col(DettaglioPdrSchema.annomese) =!= "")
}
