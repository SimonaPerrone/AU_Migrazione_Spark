package it.eng.au.ammissibilitaRendiconti.dao

import it.eng.au.ammissibilitaRendiconti.dao.IndennizziRzg2DAO.computeDelta
import it.eng.au.ammissibilitaRendiconti.dao.`trait`.OutputDAO
import it.eng.au.ammissibilitaRendiconti.model.{AggregatoTotale, ZipRzg1Metadata}
import it.eng.au.ammissibilitaRendiconti.utility.environment.Properties
import it.eng.au.indennizziMisureGasCommon.model.IndennizziRzg2
import it.eng.au.indennizziMisureGasCommon.schema.IndennizziRzg2Schema
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.LongType

import scala.util.Try

/** Tabella cig_indennizzi_rzg2. Contiene le informazioni che dovranno essere pubblicate sottoforma di file ZIP RZG2 dal processo di pubblicazione-rendiconti. */
class IndennizziRzg2DAO extends OutputDAO {
  override val tableName: String = Properties.getCigIndennizziRzg2TableName
  override val parquetPath: String = Properties.getCigIndennizziRzg2Path
  override val columns: List[String] = IndennizziRzg2Schema.getValues
  override val partitionColumn: String = IndennizziRzg2Schema.executionid.toString
  override val partitionByColumns: List[String] = List(IndennizziRzg2Schema.executionid.toString)

  def get(correctIndennizzi: RDD[(ZipRzg1Metadata, Option[AggregatoTotale])]): DataFrame = {
    Environment.sqlContext.createDataFrame(
      correctIndennizzi.map({ case (zipMetadata, aggregatoTotale) =>
        val csv = zipMetadata.csv

        val om1ID = csv.flatMap(_.om1_id)
        val om2ID = csv.flatMap(_.om2_id)
        val om3ID = csv.flatMap(_.om3_id)
        val om1SII = aggregatoTotale.flatMap(_.om1_sii)
        val om2SII = aggregatoTotale.flatMap(_.om2_sii)
        val om3SII = aggregatoTotale.flatMap(_.om3_sii)

        IndennizziRzg2(zip_file_name = zipMetadata.file.toString,
          piva_utente = zipMetadata.pivaUtente,
          piva_id = zipMetadata.pivaId,
          piva_udd = zipMetadata.pivaUdd,
          year_dir = zipMetadata.yearDir,
          month_dir = zipMetadata.monthDir,
          anno_mese_competenza = zipMetadata.annoMeseCompetenza,
          zip_timestamp = zipMetadata.timestamp,
          progressivo = zipMetadata.progressivo,
          csv_file_name = csv.map(_.fileName),
          csv_data = csv.flatMap(_.data),
          csv_id_indennizzo = csv.flatMap(_.id_indennizzo).map(_.toLong),
          csv_piva_id = csv.flatMap(_.piva_id),
          csv_rag_soc_id = csv.flatMap(_.rag_soc_id),
          csv_piva_udd = csv.flatMap(_.piva_udd),
          csv_rag_soc_udd = csv.flatMap(_.rag_soc_udd),
          csv_euro_om1 = om1ID,
          csv_euro_om2 = om2ID,
          csv_euro_om3 = om3ID,
          euro_sii_om1 = om1SII,
          euro_sii_om2 = om2SII,
          euro_sii_om3 = om3SII,
          delta_om1 = computeDelta(om1ID, om1SII),
          delta_om2 = computeDelta(om2ID, om2SII),
          delta_om3 = computeDelta(om3ID, om3SII),
          ammissibilita = zipMetadata.isAmmissibile,
          status_code = zipMetadata.statusCode,
          status_message = zipMetadata.statusMessage
        )
      })
    )
      .withColumn(IndennizziRzg2Schema.executionid, lit(Environment.executionId).cast(LongType))
      .selectExpr(IndennizziRzg2Schema.getValues: _*)
  }
}

object IndennizziRzg2DAO extends Serializable {
  def computeDelta(csvEuroID: Option[String], siiEuro: Option[Double]): Option[Double] = {
    val csvEuro = csvEuroID.map {
      case "" => 0.0
      case value => value.toDouble
    }

    Try(csvEuro.getOrElse(0.0) - siiEuro.getOrElse(0.0)).toOption
  }
}
