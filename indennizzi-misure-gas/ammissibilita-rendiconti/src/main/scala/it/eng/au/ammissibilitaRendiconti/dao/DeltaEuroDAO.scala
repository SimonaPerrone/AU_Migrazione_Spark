package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaRendiconti.schema.DeltaEuroSchema
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import it.eng.au.indennizziMisureGasCommon.schema.IndennizziRzg2Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

/** Tabella cig_delta_euro. Contiene le informazioni sugli indennizzi trasmessi, sugli indennizzi ricevuto e sul delta che eventualmente è rimasto. */
class DeltaEuroDAO extends OutputDAO {
  override val tableName: String = Properties.getCigDeltaEuroTableName
  override val parquetPath: String = Properties.getCigDeltaEuroPath
  override val columns: List[String] = DeltaEuroSchema.getValues
  override val partitionColumn: String = DeltaEuroSchema.executionid.toString
  override val partitionByColumns: List[String] = List(DeltaEuroSchema.executionid.toString)

  def get(indennizziRzg2: DataFrame): DataFrame = {
    indennizziRzg2
      .withColumnRenamed(IndennizziRzg2Schema.csv_id_indennizzo, DeltaEuroSchema.id_indennizzo)
      .withColumnRenamed(IndennizziRzg2Schema.piva_id, DeltaEuroSchema.piva_distr)
      .withColumnRenamed(IndennizziRzg2Schema.csv_rag_soc_id, DeltaEuroSchema.rag_soc_distr)
      .withColumnRenamed(IndennizziRzg2Schema.csv_rag_soc_udd, DeltaEuroSchema.rag_soc_udd)
      .withColumnRenamed(IndennizziRzg2Schema.anno_mese_competenza, DeltaEuroSchema.annomese)
      .withColumn(DeltaEuroSchema.euro_dd_om1, col(IndennizziRzg2Schema.csv_euro_om1).cast(DoubleType))
      .withColumn(DeltaEuroSchema.euro_dd_om2, col(IndennizziRzg2Schema.csv_euro_om2).cast(DoubleType))
      .withColumn(DeltaEuroSchema.euro_dd_om3, col(IndennizziRzg2Schema.csv_euro_om3).cast(DoubleType))
      .selectExpr(DeltaEuroSchema.getValues: _*)
  }
}
