package it.eng.au.ERP.trasformations.IP

import it.eng.au.ERP.schema.au.vAggreagazioneMisureIPSchema
import org.apache.spark.sql.functions.{col, lit, substring}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CalcoloPrelevatoPuntiPrelievoOrariIP {

  def aggregazioneMisureIPFiltered(dsvAggregazioneMisureIP: DataFrame,
                                   annomese: Option[String],
                                   year: Option[Int],
                                   month: Option[Int],
                                   area: Option[String],
                                   podExcluded: List[String])
                                  (implicit spark: SparkSession): DataFrame = {

    val annomeseColumn = col(vAggreagazioneMisureIPSchema.annomese).cast("string")

    val normalizedAnnomese = annomese.filter(_.trim.nonEmpty).map(_.trim)
      .orElse {
        for {
          y <- year
          m <- month
        } yield f"$y%04d$m%02d"
      }

    val dfFilteredAnnoMese = normalizedAnnomese match {
      case Some(value) =>
        dsvAggregazioneMisureIP.filter(annomeseColumn === lit(value))
      case None =>
        val filteredByYear = year match {
          case Some(yearValue) =>
            dsvAggregazioneMisureIP.filter(substring(annomeseColumn, 1, 4) === f"$yearValue%04d")
          case None => dsvAggregazioneMisureIP
        }

        month match {
          case Some(monthValue) =>
            filteredByYear.filter(substring(annomeseColumn, 5, 2) === f"$monthValue%02d")
          case None => filteredByYear
        }
    }

    val dfFilteredArea = area match {
      case Some(areaValue) if areaValue.trim.nonEmpty =>
        dfFilteredAnnoMese.filter(col(vAggreagazioneMisureIPSchema.t_area) === lit(areaValue.trim))
      case _ => dfFilteredAnnoMese
    }

    val normalizedPodExcluded = podExcluded.flatMap(pod => Option(pod).map(_.trim)).filter(_.nonEmpty)

    if (normalizedPodExcluded.nonEmpty) {
      dfFilteredArea.filter(!col(vAggreagazioneMisureIPSchema.t_pod).isin(normalizedPodExcluded.distinct: _*))
    } else {
      dfFilteredArea
    }
  }
}
