package it.sferanet.au.filterPdr

import it.sferanet.au.schema.{CaFinalSchema, RcuGasMassivoPSchema}
import it.sferanet.au.utilities.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

import java.time.{LocalDate, Month}

class Filter51EventOggettoVariazione extends FilterPdr {

  override def getPdrs: RDD[String] = {
    val rcuGasMassivoP: DataFrame = getRcuGasMassivoP
    val caFinal: DataFrame = getCaFinal

    val today: LocalDate = LocalDate.now()
    val pastJuly: LocalDate = getPastJuly(today)
    val currentThermalYear = getCurrentThermalYear(today)

    // Filtra tutti i pdr attivati dal 1 Luglio Normativo (scorso 1 Luglio) ad oggi
    val pdrsActivatedFromPastJuly = rcuGasMassivoP
      .select(RcuGasMassivoPSchema.t_codice_pdr, RcuGasMassivoPSchema.d_data_inizio_for)
      .where(col(RcuGasMassivoPSchema.d_data_inizio_for).cast(DateType).between(to_date(lit(pastJuly.toString)), to_date(lit(today.toString))))
      .select(RcuGasMassivoPSchema.t_codice_pdr)
      .distinct()

    // filtra tutti i pdr che non sono mai entrati nel calcolo della CA per l'anno termico corrente
    // (anno_competenza diverso dall'anno termico corrente). NB. anno termico = dal 1/10/YYYY al 30/09/(YYYY+1)
    caFinal.where(col(CaFinalSchema.anno_competenza) === lit(currentThermalYear)) //tutti i pdr nella ca_final per questo anno termico
      .select(CaFinalSchema.codice_pdr)
      .distinct()
      .join(pdrsActivatedFromPastJuly, pdrsActivatedFromPastJuly.col(RcuGasMassivoPSchema.t_codice_pdr) === caFinal.col(CaFinalSchema.codice_pdr), "right")
      .where(col(CaFinalSchema.codice_pdr).isNull) //tutti i pdr senza ca per questo anno termico
      .select(RcuGasMassivoPSchema.t_codice_pdr)
      .rdd
      .map(_.getString(0))
  }

  /**
   * Get the past 1st July from the date passed as input.
   * Assume today is dd-MM-yyyy then the method returns 01-July-(YYYY-1) if MM is before July, 01-July-YYYY otherwise
   *
   * @param today represent today LocalDate, like LocalDate.now
   * @return the past 1st of July starting from input param today
   * */
  def getPastJuly(today: LocalDate): LocalDate = {
    //if today's month is less than July then we need to get the 1st July of the past year
    if (today.getMonth.compareTo(Month.JULY) < 0) {
      today.minusYears(1)
        .withMonth(Constants.JULY)
        .withDayOfMonth(1)
    }
    //if today's month is equal to or greater than July then we need to get the 1st July of this current year
    else {
      today.withMonth(Constants.JULY)
        .withDayOfMonth(1)
    }
  }
}
