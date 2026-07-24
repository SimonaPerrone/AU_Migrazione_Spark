package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.dao.`trait`.DAO
import it.eng.au.ammissibilitaRendiconti.model.AggregatoTotale
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import it.eng.au.indennizziMisureGasCommon.schema.AggregatoTotaleSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.util.Try

/** Tabella cig_aggregato_totale. Viene utilizzata per verificare la regola [[ruleValidateIdIndennizzo]].*/
class AggregatoTotaleDAO extends DAO {
  override val tableName: String = Properties.getCigAggregatoTotaleTableName
  override val columns: List[String] = AggregatoTotaleSchema.getValues
  override val partitionColumn: String = AggregatoTotaleSchema.executionid

  def get: RDD[AggregatoTotale] = {
    val df = readTable
      .select(
        AggregatoTotaleSchema.id_indennizzo,
        AggregatoTotaleSchema.piva_udd,
        AggregatoTotaleSchema.piva_distr,
        AggregatoTotaleSchema.indennizzo_om1,
        AggregatoTotaleSchema.indennizzo_om2,
        AggregatoTotaleSchema.indennizzo_om3
      )

    val mapFunc: Row => AggregatoTotale = (r: Row) => {
      AggregatoTotale(
        id_indennizzo = r.getAs[Long](AggregatoTotaleSchema.id_indennizzo),
        piva_udd = r.getAs[String](AggregatoTotaleSchema.piva_udd),
        piva_id = r.getAs[String](AggregatoTotaleSchema.piva_distr),
        om1_sii = Try(r.getAs[Double](AggregatoTotaleSchema.indennizzo_om1)).toOption,
        om2_sii = Try(r.getAs[Double](AggregatoTotaleSchema.indennizzo_om2)).toOption,
        om3_sii = Try(r.getAs[Double](AggregatoTotaleSchema.indennizzo_om3)).toOption
      )
    }

    df.rdd.map(mapFunc)
  }
}
