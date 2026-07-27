
package it.eng.au.portale_consumi_ee.trasformations

import it.eng.au.portale_consumi_ee.common.utility.environment.Environment
import it.eng.au.portale_consumi_ee.environment.EnvironmentMisure
import it.eng.au.portale_consumi_ee.model.Forniture.MisureElettricheModel
import it.eng.au.portale_consumi_ee.model.misure.etlStage3M2ProposedModel
import it.eng.au.portale_consumi_ee.schema.Forniture.MisureElettricheSchema
import it.eng.au.portale_consumi_ee.schema.misure.etlStage3M2ProposedSchema
import it.eng.au.portale_consumi_ee.utility.functions.argumentsUtilitiesExport
import org.apache.spark.sql.functions.{coalesce, col, concat, lit}
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ExportOpertion {
  val spark: SparkSession = EnvironmentMisure.getSpark

  import spark.implicits._

  def writeNewData(dataset: Dataset[etlStage3M2ProposedModel]): Dataset[MisureElettricheModel] = {

    //set datasetNew as Dataset[MisureElettricheModel]

//    val newDataSetUp =
      dataset
      .withColumn(MisureElettricheSchema._id, concat(col(etlStage3M2ProposedSchema.n_id_fornitura),lit("_"),col(etlStage3M2ProposedSchema.competenza_consumi)))
      .withColumn(MisureElettricheSchema.codice_fornitura, col(etlStage3M2ProposedSchema.n_id_fornitura))
      .withColumn(MisureElettricheSchema.competenza_consumi, col(etlStage3M2ProposedSchema.competenza_consumi))
      .withColumnRenamed(etlStage3M2ProposedSchema.pod, MisureElettricheSchema.pod)
      .withColumnRenamed(etlStage3M2ProposedSchema.misure_orarie, MisureElettricheSchema.misure_orarie)
      .withColumnRenamed(etlStage3M2ProposedSchema.misure_mensili, MisureElettricheSchema.misure_mensili)
      .withColumnRenamed(etlStage3M2ProposedSchema.misure_non_orarie, MisureElettricheSchema.misure_non_orarie)
      .withColumnRenamed(etlStage3M2ProposedSchema.volture, MisureElettricheSchema.volture)
      .withColumnRenamed(etlStage3M2ProposedSchema.autoletture, MisureElettricheSchema.autoletture)
      .selectExpr(MisureElettricheSchema.getValues: _*)
      .as[MisureElettricheModel]

    // Now override misure_mensili as the coalesced value
//    val finalDataSet = newDataSetUp
//      .withColumn(MisureElettricheSchema.misure_mensili,
//        coalesce(col(MisureElettricheSchema.misure_mensili), col(MisureElettricheSchema.misure_non_orarie))
//      )
//
//    val structCols = Seq(
//      MisureElettricheSchema.misure_mensili,
//      MisureElettricheSchema.misure_non_orarie
//    )
//
//    val withExplicitStructs = structCols.foldLeft(finalDataSet) { (df, colName) =>
//      val withStruct = argumentsUtilitiesExport.forceStructFieldsToAppear(df, colName)
//      argumentsUtilitiesExport.dropFullyNullStruct(withStruct, colName)
//    }
//
//
//
//    val result = withExplicitStructs
//      .selectExpr(MisureElettricheSchema.getValues: _*)
//      .as[MisureElettricheModel]
//
//    result
  }


}
