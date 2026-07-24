package it.eng.au.calcoloIndennizzi.dao.output

import it.eng.au.calcoloIndennizzi.schema.cig.DettaglioOM3Schema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import org.apache.spark.sql.DataFrame

class DettaglioOM3DAO extends OutputDAO {
  val tableName: String = Properties.getDettaglioOM3TableName
  val parquetPath: String = Properties.getDettaglioOM3Path
  val columns: List[String] = DettaglioOM3Schema.getValues
  val partitionColumns: List[String] = List(DettaglioOM3Schema.annomese, DettaglioOM3Schema.executionid)

  def get(aggregatoTotale: DataFrame): DataFrame = {
    aggregatoTotale
      .withColumnRenamed(AggregatoTotaleSchema.percentage_lower_bound_om3, DettaglioOM3Schema.percentage_lower_bound)
      .withColumnRenamed(AggregatoTotaleSchema.percentage_upper_bound_om3, DettaglioOM3Schema.percentage_upper_bound)
      .withColumnRenamed(AggregatoTotaleSchema.target_percentage_om3, DettaglioOM3Schema.target_percentage)
      .withColumnRenamed(AggregatoTotaleSchema.achieved_percentage_om3, DettaglioOM3Schema.achieved_percentage)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_g_om1, DettaglioOM3Schema.pdr_base)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_target_om3, DettaglioOM3Schema.pdr_target)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_g_om3, DettaglioOM3Schema.pdr_count)
      .withColumnRenamed(AggregatoTotaleSchema.delta_pdr_om3, DettaglioOM3Schema.delta_pdr)
      .withColumnRenamed(AggregatoTotaleSchema.euro_fee_per_pdr_om3, DettaglioOM3Schema.euro_fee_per_pdr)
      .withColumnRenamed(AggregatoTotaleSchema.indennizzo_om3, DettaglioOM3Schema.indennizzo)
      .selectExpr(DettaglioOM3Schema.getValues: _*)
  }
}
