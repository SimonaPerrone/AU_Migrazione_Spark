package it.sferanet.au.filterPdr

import it.sferanet.au.schema.{CaPreFinalSchema, CaSchema, CodProfStdDaTdsSchema, PdrMassivoSchema}
import it.sferanet.au.utilities.Environment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


class FilterPerimetroAggRic(pdrMassivo: DataFrame) extends FilterPdr {

  /**
   * Estrae il perimetro di PdR per il filtro Agg Ric. In particolare,
   *  - dalla ca_pre_final, seleziona per ogni PdR il record più recente (ordinando per executionid), e di questi mantiene soltanto i PdR con trattamento Y
   *  - dalla massivo, seleziona i PdR con trattamento Y
   *  - dalla ca selezione i PdR non nulli
   *  - dalla cod_prof_std_da_tds estrae i PdR con anno competenza uguale all'anno competenza in input (pdr_massivo.anno_competenza)
   *
   * Successivamente, esegue successive join con la ca_pre_final filtrata come tabella di sinistra:
   *  - left join con la ca per selezionare i PdR con idCaErrorCode null (quindi PdR non precedentemente calcolati) oppure in [5,9]
   *  - left join con la cod_prof_std_da_tds per rimuovere i PdR presenti in quest'ultima dalla ca_pre_final
   *  - inner join con la massivo perché in essa è presente il perimetro base dei PdR
   * @return la lista dei PdR di cui effettuare il calcolo della ca (consumo annuo)
   */
  override def getPdrs: RDD[String] = {
    pdrMassivo.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val caPreFinal: DataFrame = getCaPreFinal
    val ca: DataFrame = getCa
    val codProfStdDaTds: DataFrame = getCodProfStdDaTds
    val annoCompetenza: String = Environment.getMassivoAnnoCompetenza
    val pdrWindow: WindowSpec = Window.partitionBy(CaPreFinalSchema.codice_pdr).orderBy(col(CaPreFinalSchema.executionid).desc)

    val caPreFinalP = caPreFinal
      .withColumn(CaPreFinalSchema.trattamento, upper(coalesce(col(CaPreFinalSchema.trattamento_forced), col(CaPreFinalSchema.trattamento))))
      .select(CaPreFinalSchema.codice_pdr, CaPreFinalSchema.executionid, CaPreFinalSchema.anno_competenza, CaPreFinalSchema.trattamento)
      .where(
        col(CaPreFinalSchema.executionid).isNotNull &&
          col(CaPreFinalSchema.codice_pdr).isNotNull &&
          col(CaPreFinalSchema.anno_competenza) === annoCompetenza)
      .withColumn("rn", row_number().over(pdrWindow))
      .where(col("rn") === 1)
      .where(col(CaPreFinalSchema.trattamento) === "Y")
      .select(CaPreFinalSchema.executionid, CaPreFinalSchema.codice_pdr)

    //filter massivo
    val massivoFiltered = pdrMassivo
      .where(col(PdrMassivoSchema.trattamento) === "Y")
      .select(PdrMassivoSchema.codice_pdr)

    val caFiltered = ca
      .select(CaSchema.pdr, CaSchema.executionid, CaSchema.idCaErrorCode)
      .where(col(CaSchema.pdr).isNotNull && col(CaSchema.executionid).isNotNull)
      .distinct()

    val pdrToBeRemoved = codProfStdDaTds
      .where(col(CodProfStdDaTdsSchema.anno_competenza) === annoCompetenza)
      .select(CodProfStdDaTdsSchema.codice_pdr)
      .distinct()


    caPreFinalP
      .join(caFiltered, caPreFinal(CaPreFinalSchema.codice_pdr) === caFiltered(CaSchema.pdr) && caPreFinal(CaPreFinalSchema.executionid) === caFiltered(CaSchema.executionid), "left_outer")
      .drop(caFiltered(CaSchema.pdr))
      .drop(caFiltered(CaSchema.executionid))
      .where(col(CaSchema.idCaErrorCode).isNull || col(CaSchema.idCaErrorCode).isin(5, 9))
      .select(CaPreFinalSchema.codice_pdr)
      .distinct()
      .join(pdrToBeRemoved, caPreFinalP(CaPreFinalSchema.codice_pdr) === pdrToBeRemoved(CodProfStdDaTdsSchema.codice_pdr), "left_outer")
      .where(pdrToBeRemoved(CodProfStdDaTdsSchema.codice_pdr).isNull)
      .join(massivoFiltered, caPreFinalP(CaPreFinalSchema.codice_pdr) === massivoFiltered(PdrMassivoSchema.codice_pdr))
      .select(caPreFinalP(CaPreFinalSchema.codice_pdr))
      .rdd
      .map(_.getString(0))
  }
}
