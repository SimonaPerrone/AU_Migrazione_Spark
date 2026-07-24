package it.eng.au.ammissibilitaRendiconti.controller

import it.eng.au.ammissibilitaRendiconti.model.{AggregatoTotale, ZipRzg1Metadata}
import org.apache.spark.rdd.RDD

object JoinController extends Serializable {
  /** Esegue la join tra i metadati degli zip e la tabella cig_aggregato_totale. Le informazioni estratte dall'aggregato totale servono
   * per la regola [[ruleValidateIdIndennizzo]]. */
  def joinRzg1Indennizzi(zipRzg1Metadata: RDD[ZipRzg1Metadata], aggregatoTotale: RDD[AggregatoTotale]): RDD[(ZipRzg1Metadata, Option[AggregatoTotale])] = {
    val aggregatoKeyBy = aggregatoTotale.keyBy(indennizzo => (indennizzo.piva_id, indennizzo.piva_udd, indennizzo.id_indennizzo.toString))

    zipRzg1Metadata
      .keyBy(zipRzg1 => (zipRzg1.pivaId, zipRzg1.pivaUdd, zipRzg1.csv.flatMap(_.id_indennizzo).orNull))
      .leftOuterJoin(aggregatoKeyBy)
      .values
  }
}