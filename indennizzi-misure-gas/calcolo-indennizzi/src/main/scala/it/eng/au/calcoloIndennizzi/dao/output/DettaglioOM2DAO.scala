package it.eng.au.calcoloIndennizzi.dao.output

import it.eng.au.calcoloIndennizzi.schema.cig.DettaglioOM2Schema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import org.apache.spark.sql.DataFrame

class DettaglioOM2DAO extends OutputDAO {
  val tableName: String = Properties.getDettaglioOM2TableName
  val parquetPath: String = Properties.getDettaglioOM2Path
  val columns: List[String] = DettaglioOM2Schema.getValues
  val partitionColumns: List[String] = List(DettaglioOM2Schema.annomese, DettaglioOM2Schema.executionid)
  
  def get(aggregatoTotale: DataFrame): DataFrame = {
    aggregatoTotale
      .withColumnRenamed(AggregatoTotaleSchema.percentage_lower_bound_om2, DettaglioOM2Schema.percentage_lower_bound)
      .withColumnRenamed(AggregatoTotaleSchema.percentage_upper_bound_om2, DettaglioOM2Schema.percentage_upper_bound)
      .withColumnRenamed(AggregatoTotaleSchema.target_percentage_om2, DettaglioOM2Schema.target_percentage)
      .withColumnRenamed(AggregatoTotaleSchema.achieved_percentage_om2, DettaglioOM2Schema.achieved_percentage)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_g_om1, DettaglioOM2Schema.pdr_base)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_target_om2, DettaglioOM2Schema.pdr_target)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_g_om2, DettaglioOM2Schema.pdr_count)
      .withColumnRenamed(AggregatoTotaleSchema.delta_pdr_om2, DettaglioOM2Schema.delta_pdr)
      .withColumnRenamed(AggregatoTotaleSchema.euro_fee_per_pdr_om2, DettaglioOM2Schema.euro_fee_per_pdr)
      .withColumnRenamed(AggregatoTotaleSchema.indennizzo_om2, DettaglioOM2Schema.indennizzo)
      .selectExpr(DettaglioOM2Schema.getValues: _*)
  }
}
