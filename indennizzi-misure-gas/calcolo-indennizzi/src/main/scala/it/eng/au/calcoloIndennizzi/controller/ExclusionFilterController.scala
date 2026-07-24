package it.eng.au.calcoloIndennizzi.controller

import it.eng.au.calcoloIndennizzi.model.measure.Tgl
import it.eng.au.calcoloIndennizzi.schema.ExclusionFilterSchema
import it.eng.au.calcoloIndennizzi.schema.rcugas.RcugasMassivoPSchema
import it.eng.au.calcoloIndennizzi.utility.Properties
import it.eng.au.indennizziMisureGasCommon.utility.dataframe.DataFrameUtils.{isNotNullNorEmpty, isNullOrEmpty}
import it.eng.au.indennizziMisureGasCommon.utility.environment.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col}

/** Implementa alcune funzioni utilizzate per l'esclusione di PdR e/o TGL dal processo. In particolare,
 *  - i PdR esclusi saranno completamente rimossi dal perimetro;
 *  - le TGL o le coppie (PdR, TGL) escluse verranno soltanto rimosse dal calcolo degli indennizzi.
 *  */
object ExclusionFilterController extends Serializable {
  private val isEnabled: Boolean = Properties.isExclusionFilterEnabled

  private lazy val exclusionFileDf: DataFrame = Environment.sqlContext
    .read
    .option("header", value = true)
    .schema(ExclusionFilterSchema.schema)
    .csv(Properties.getExclusionFilterPath)
    .cache

  /** Returns `rcugasMassivo` DataFrame without the list of PdRs included in [[exclusionFileDf]]. See `ExclusionFilterController` for more details.
   *
   * @param rcugasMassivo DataFrame from which remove the PdRs.
   */
  def excludePdrs(rcugasMassivo: DataFrame): DataFrame = {
    if (isEnabled) {
      val pdrsToExclude = exclusionFileDf
        .where(col(ExclusionFilterSchema.pdr).isNotNull)
        .where(col(ExclusionFilterSchema.file).isNull)
        .select(ExclusionFilterSchema.pdr)
        .distinct

      rcugasMassivo
        .join(broadcast(pdrsToExclude), rcugasMassivo(RcugasMassivoPSchema.t_codice_pdr) === pdrsToExclude(ExclusionFilterSchema.pdr), "left")
        .where(pdrsToExclude(ExclusionFilterSchema.pdr).isNull)
        .drop(pdrsToExclude(ExclusionFilterSchema.pdr))
    }
    else rcugasMassivo
  }


  /**
   * Esclude dal calcolo degli indennizzi le TGL o le coppie (PdR, TGL) contenute nel file di esclusione.
   * @param measures RDD delle TGL lette
   * @return lo stesso RDD senza le TGL o le coppie (PdR, TGL) da escludere
   */
  def excludeTgls(measures: RDD[Tgl]): RDD[Tgl] = {
    if (isEnabled) {
      val fileToExcludeRDD: RDD[(String, Boolean)] = exclusionFileDf
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.file)))
        .where(isNullOrEmpty(col(ExclusionFilterSchema.pdr)))
        .select(col(ExclusionFilterSchema.file))
        .distinct
        .rdd
        .map(row => (row.getAs[String](ExclusionFilterSchema.file), true))

      val pdrWithFileToExcludeRDD: RDD[((String, String), Boolean)] = exclusionFileDf
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.pdr)))
        .where(isNotNullNorEmpty(col(ExclusionFilterSchema.file)))
        .select(col(ExclusionFilterSchema.pdr), col(ExclusionFilterSchema.file))
        .distinct
        .rdd
        .map(row => ((row.getAs[String](ExclusionFilterSchema.pdr), row.getAs[String](ExclusionFilterSchema.file)), true))

      measures.map(tgl => ((tgl.pdr, tgl.localFile.getOrElse("-1")), tgl)) //keyBy pdr and local_file
        .leftOuterJoin(pdrWithFileToExcludeRDD) //left join with couples (pdr, file) to exclude
        .filter({ case ((pdr, file), (flow, hasMatched)) => hasMatched.isEmpty }) //exclude all the measures with a match
        .map({ case ((pdr, file), (flow, hasMatched)) => (file, flow) }) //key by local_file
        .leftOuterJoin(fileToExcludeRDD) //left join with files to exclude
        .filter({ case (file, (flow, hasMatched)) => hasMatched.isEmpty }) //exclude all the measures with a match
        .map({ case (pdr, (flow, hasMatched)) => flow }) //get the measures
    } else {
      measures
    }
  }
}
