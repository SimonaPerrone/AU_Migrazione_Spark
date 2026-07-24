package it.eng.au.calcoloIndennizzi.dao.output

import it.eng.au.calcoloIndennizzi.schema.cig.DettaglioOM1Schema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import org.apache.spark.sql.DataFrame

class DettaglioOM1DAO extends OutputDAO {
  val tableName: String = Properties.getDettaglioOM1TableName
  val parquetPath: String = Properties.getDettaglioOM1Path
  val columns: List[String] = DettaglioOM1Schema.getValues
  val partitionColumns: List[String] = List(DettaglioOM1Schema.annomese, DettaglioOM1Schema.executionid)

  def get(aggregatoTotale: DataFrame): DataFrame = {
    aggregatoTotale
      .withColumnRenamed(AggregatoTotaleSchema.target_percentage_om1, DettaglioOM1Schema.target_percentage)
      .withColumnRenamed(AggregatoTotaleSchema.achieved_percentage_om1, DettaglioOM1Schema.achieved_percentage)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_g, DettaglioOM1Schema.pdr_base)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_target_om1, DettaglioOM1Schema.pdr_target)
      .withColumnRenamed(AggregatoTotaleSchema.pdr_g_om1, DettaglioOM1Schema.pdr_count)
      .withColumnRenamed(AggregatoTotaleSchema.delta_pdr_om1, DettaglioOM1Schema.delta_pdr)
      .withColumnRenamed(AggregatoTotaleSchema.euro_fee_per_pdr_om1, DettaglioOM1Schema.euro_fee_per_pdr)
      .withColumnRenamed(AggregatoTotaleSchema.indennizzo_om1, DettaglioOM1Schema.indennizzo)
      .selectExpr(DettaglioOM1Schema.getValues: _*)
  }
}
